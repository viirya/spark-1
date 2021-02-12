/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.streaming.state

import org.apache.hadoop.conf.Configuration

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Literal, SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.types.{BooleanType, LongType, StructField, StructType}
import org.apache.spark.util.NextIterator

class StreamingStateManager(
    inputValueAttributes: Seq[Attribute],
    keys: Seq[Expression],
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration,
    partitionId: Int,
    stateFormatVersion: Int) extends Logging {

  import StreamingStateManagerBase._

  /** Get all the values of a key */
  def get(key: UnsafeRow): Iterator[UnsafeRow] = {
    val numValues = keyToNumValues.get(key)
    keyWithIndexToValue.getAll(key, numValues).map(_.value)
  }

  /** Append a new value to the key */
  def append(key: UnsafeRow, value: UnsafeRow, matched: Boolean): Unit = {
    val numExistingValues = keyToNumValues.get(key)
    keyWithIndexToValue.put(key, numExistingValues, value, matched)
    keyToNumValues.put(key, numExistingValues + 1)
  }

  /** Commit all the changes to all the state stores */
  def commit(): Unit = {
    keyToNumValues.commit()
    keyWithIndexToValue.commit()
  }

  /** Abort any changes to the state stores if needed */
  def abortIfNeeded(): Unit = {
    keyToNumValues.abortIfNeeded()
    keyWithIndexToValue.abortIfNeeded()
  }

  /** Get the combined metrics of all the state stores */
  def metrics: StateStoreMetrics = {
    val keyToNumValuesMetrics = keyToNumValues.metrics
    val keyWithIndexToValueMetrics = keyWithIndexToValue.metrics

    StateStoreMetrics(
      keyWithIndexToValueMetrics.numKeys,       // represent each buffered row only once
      keyToNumValuesMetrics.memoryUsedBytes + keyWithIndexToValueMetrics.memoryUsedBytes,
      keyWithIndexToValueMetrics.customMetrics
    )
  }

  protected def getStateStoreName(storeType: StateStoreType): String = s"$storeType"

  protected val keySchema = StructType(
    keys.zipWithIndex.map { case (k, i) => StructField(s"field$i", k.dataType, k.nullable) })
  private val keyAttributes = keySchema.toAttributes
  protected val keyToNumValues = new KeyToNumValuesStore()
  protected val keyWithIndexToValue = new KeyWithIndexToValueStore(stateFormatVersion)

  // Clean up any state store resources if necessary at the end of the task
  Option(TaskContext.get()).foreach { _.addTaskCompletionListener[Unit] { _ => abortIfNeeded() } }

  /** Helper trait for invoking common functionalities of a state store. */
  protected abstract class StateStoreHandler(stateStoreType: StateStoreType) extends Logging {

    /** StateStore that the subclasses of this class is going to operate on */
    protected def stateStore: StateStore

    def commit(): Unit = {
      stateStore.commit()
      logDebug("Committed, metrics = " + stateStore.metrics)
    }

    def abortIfNeeded(): Unit = {
      if (!stateStore.hasCommitted) {
        logInfo(s"Aborted store ${stateStore.id}")
        stateStore.abort()
      }
    }

    def metrics: StateStoreMetrics = stateStore.metrics

    /** Get the StateStore with the given schema */
    protected def getStateStore(keySchema: StructType, valueSchema: StructType): StateStore = {
      val storeProviderId = StateStoreProviderId(
        stateInfo.get, partitionId, getStateStoreName(stateStoreType))
      val store = StateStore.get(
        storeProviderId, keySchema, valueSchema, None,
        stateInfo.get.storeVersion, storeConf, hadoopConf)
      logInfo(s"Loaded store ${store.id}")
      store
    }
  }

  /**
   * Helper class for representing data returned by [[KeyWithIndexToValueStore]].
   * Designed for object reuse.
   */
  protected class KeyAndNumValues(var key: UnsafeRow = null, var numValue: Long = 0) {
    def withNew(newKey: UnsafeRow, newNumValues: Long): this.type = {
      this.key = newKey
      this.numValue = newNumValues
      this
    }
  }

  /** A wrapper around a [[StateStore]] that stores [key -> number of values]. */
  protected class KeyToNumValuesStore extends StateStoreHandler(KeyToNumValuesType) {
    private val longValueSchema = new StructType().add("value", "long")
    private val longToUnsafeRow = UnsafeProjection.create(longValueSchema)
    private val valueRow = longToUnsafeRow(new SpecificInternalRow(longValueSchema))
    protected val stateStore: StateStore = getStateStore(keySchema, longValueSchema)

    /** Get the number of values the key has */
    def get(key: UnsafeRow): Long = {
      val longValueRow = stateStore.get(key)
      if (longValueRow != null) longValueRow.getLong(0) else 0L
    }

    /** Set the number of values the key has */
    def put(key: UnsafeRow, numValues: Long): Unit = {
      require(numValues > 0)
      valueRow.setLong(0, numValues)
      stateStore.put(key, valueRow)
    }

    def remove(key: UnsafeRow): Unit = {
      stateStore.remove(key)
    }

    def iterator: Iterator[KeyAndNumValues] = {
      val keyAndNumValues = new KeyAndNumValues()
      stateStore.getRange(None, None).map { case pair =>
        keyAndNumValues.withNew(pair.key, pair.value.getLong(0))
      }
    }
  }

  /**
   * Helper class for representing data returned by [[KeyWithIndexToValueStore]].
   * Designed for object reuse.
   */
  protected class KeyWithIndexAndValue(
      var key: UnsafeRow = null,
      var valueIndex: Long = -1,
      var value: UnsafeRow = null,
      var matched: Boolean = false) {

    def withNew(
        newKey: UnsafeRow,
        newIndex: Long,
        newValue: UnsafeRow,
        newMatched: Boolean): this.type = {
      this.key = newKey
      this.valueIndex = newIndex
      this.value = newValue
      this.matched = newMatched
      this
    }

    def withNew(
        newKey: UnsafeRow,
        newIndex: Long,
        newValue: ValueAndMatchPair): this.type = {
      this.key = newKey
      this.valueIndex = newIndex
      if (newValue != null) {
        this.value = newValue.value
        this.matched = newValue.matched
      } else {
        this.value = null
        this.matched = false
      }
      this
    }
  }

  private trait KeyWithIndexToValueRowConverter {
    /** Defines the schema of the value row (the value side of K-V in state store). */
    def valueAttributes: Seq[Attribute]

    /**
     * Convert the value row to (actual value, match) pair.
     *
     * NOTE: implementations should ensure the result row is NOT reused during execution, so
     * that caller can safely read the value in any time.
     */
    def convertValue(value: UnsafeRow): ValueAndMatchPair

    /**
     * Build the value row from (actual value, match) pair. This is expected to be called just
     * before storing to the state store.
     *
     * NOTE: depending on the implementation, the result row "may" be reused during execution
     * (to avoid initialization of object), so the caller should ensure that the logic doesn't
     * affect by such behavior. Call copy() against the result row if needed.
     */
    def convertToValueRow(value: UnsafeRow, matched: Boolean): UnsafeRow
  }

  private object KeyWithIndexToValueRowConverter {
    def create(version: Int): KeyWithIndexToValueRowConverter = version match {
      case 1 => new KeyWithIndexToValueRowConverterFormatV1()
      case 2 => new KeyWithIndexToValueRowConverterFormatV2()
      case _ => throw new IllegalArgumentException("Incorrect state format version! " +
        s"version $version")
    }
  }

  private class KeyWithIndexToValueRowConverterFormatV1 extends KeyWithIndexToValueRowConverter {
    override val valueAttributes: Seq[Attribute] = inputValueAttributes

    override def convertValue(value: UnsafeRow): ValueAndMatchPair = {
      if (value != null) ValueAndMatchPair(value, false) else null
    }

    override def convertToValueRow(value: UnsafeRow, matched: Boolean): UnsafeRow = value
  }

  private class KeyWithIndexToValueRowConverterFormatV2 extends KeyWithIndexToValueRowConverter {
    private val valueWithMatchedExprs = inputValueAttributes :+ Literal(true)
    private val indexOrdinalInValueWithMatchedRow = inputValueAttributes.size

    private val valueWithMatchedRowGenerator = UnsafeProjection.create(valueWithMatchedExprs,
      inputValueAttributes)

    override val valueAttributes: Seq[Attribute] = inputValueAttributes :+
      AttributeReference("matched", BooleanType)()

    // Projection to generate key row from (value + matched) row
    private val valueRowGenerator = UnsafeProjection.create(
      inputValueAttributes, valueAttributes)

    override def convertValue(value: UnsafeRow): ValueAndMatchPair = {
      if (value != null) {
        ValueAndMatchPair(valueRowGenerator(value).copy(),
          value.getBoolean(indexOrdinalInValueWithMatchedRow))
      } else {
        null
      }
    }

    override def convertToValueRow(value: UnsafeRow, matched: Boolean): UnsafeRow = {
      val row = valueWithMatchedRowGenerator(value)
      row.setBoolean(indexOrdinalInValueWithMatchedRow, matched)
      row
    }
  }

  /**
   * A wrapper around a [[StateStore]] that stores the mapping; the mapping depends on the
   * state format version - please refer implementations of [[KeyWithIndexToValueRowConverter]].
   */
  protected class KeyWithIndexToValueStore(stateFormatVersion: Int)
    extends StateStoreHandler(KeyWithIndexToValueType) {

    private val keyWithIndexExprs = keyAttributes :+ Literal(1L)
    private val keyWithIndexSchema = keySchema.add("index", LongType)
    private val indexOrdinalInKeyWithIndexRow = keyAttributes.size

    // Projection to generate (key + index) row from key row
    private val keyWithIndexRowGenerator = UnsafeProjection.create(keyWithIndexExprs, keyAttributes)

    // Projection to generate key row from (key + index) row
    private val keyRowGenerator = UnsafeProjection.create(
      keyAttributes, keyAttributes :+ AttributeReference("index", LongType)())

    private val valueRowConverter = KeyWithIndexToValueRowConverter.create(stateFormatVersion)

    protected val stateStore = getStateStore(keyWithIndexSchema,
      valueRowConverter.valueAttributes.toStructType)

    def get(key: UnsafeRow, valueIndex: Long): ValueAndMatchPair = {
      valueRowConverter.convertValue(stateStore.get(keyWithIndexRow(key, valueIndex)))
    }

    /**
     * Get all values and indices for the provided key.
     * Should not return null.
     */
    def getAll(key: UnsafeRow, numValues: Long): Iterator[KeyWithIndexAndValue] = {
      val keyWithIndexAndValue = new KeyWithIndexAndValue()
      var index = 0
      new NextIterator[KeyWithIndexAndValue] {
        override protected def getNext(): KeyWithIndexAndValue = {
          if (index >= numValues) {
            finished = true
            null
          } else {
            val keyWithIndex = keyWithIndexRow(key, index)
            val valuePair = valueRowConverter.convertValue(stateStore.get(keyWithIndex))
            keyWithIndexAndValue.withNew(key, index, valuePair)
            index += 1
            keyWithIndexAndValue
          }
        }

        override protected def close(): Unit = {}
      }
    }

    /** Put new value for key at the given index */
    def put(key: UnsafeRow, valueIndex: Long, value: UnsafeRow, matched: Boolean): Unit = {
      val keyWithIndex = keyWithIndexRow(key, valueIndex)
      val valueWithMatched = valueRowConverter.convertToValueRow(value, matched)
      stateStore.put(keyWithIndex, valueWithMatched)
    }

    /**
     * Remove key and value at given index. Note that this will create a hole in
     * (key, index) and it is upto the caller to deal with it.
     */
    def remove(key: UnsafeRow, valueIndex: Long): Unit = {
      stateStore.remove(keyWithIndexRow(key, valueIndex))
    }

    /** Remove all values (i.e. all the indices) for the given key. */
    def removeAllValues(key: UnsafeRow, numValues: Long): Unit = {
      var index = 0
      while (index < numValues) {
        stateStore.remove(keyWithIndexRow(key, index))
        index += 1
      }
    }

    def iterator: Iterator[KeyWithIndexAndValue] = {
      val keyWithIndexAndValue = new KeyWithIndexAndValue()
      stateStore.getRange(None, None).map { pair =>
        val valuePair = valueRowConverter.convertValue(pair.value)
        keyWithIndexAndValue.withNew(
          keyRowGenerator(pair.key), pair.key.getLong(indexOrdinalInKeyWithIndexRow), valuePair)
        keyWithIndexAndValue
      }
    }

    /** Generated a row using the key and index */
    private def keyWithIndexRow(key: UnsafeRow, valueIndex: Long): UnsafeRow = {
      val row = keyWithIndexRowGenerator(key)
      row.setLong(indexOrdinalInKeyWithIndexRow, valueIndex)
      row
    }
  }
}

object StreamingStateManagerBase {

  private[state] sealed trait StateStoreType

  private[state] case object KeyToNumValuesType extends StateStoreType {
    override def toString(): String = "keyToNumValues"
  }

  private[state] case object KeyWithIndexToValueType extends StateStoreType {
    override def toString(): String = "keyWithIndexToValue"
  }

  /** Helper class for representing data (value, matched). */
  case class ValueAndMatchPair(value: UnsafeRow, matched: Boolean)

  /**
   * Helper class for representing data key to (value, matched).
   * Designed for object reuse.
   */
  case class KeyToValuePair(
      var key: UnsafeRow = null,
      var value: UnsafeRow = null,
      var matched: Boolean = false) {
    def withNew(newKey: UnsafeRow, newValue: UnsafeRow, newMatched: Boolean): this.type = {
      this.key = newKey
      this.value = newValue
      this.matched = newMatched
      this
    }

    def withNew(newKey: UnsafeRow, newValue: ValueAndMatchPair): this.type = {
      this.key = newKey
      if (newValue != null) {
        this.value = newValue.value
        this.matched = newValue.matched
      } else {
        this.value = null
        this.matched = false
      }
      this
    }
  }
}
