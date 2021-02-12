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

import java.util.Locale

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, JoinedRow, UnsafeRow}
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper._
import org.apache.spark.util.NextIterator

/**
 * Helper class to manage state required by a single side of
 * [[org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinExec]].
 * The interface of this class is basically that of a multi-map:
 * - Get: Returns an iterator of multiple values for given key
 * - Append: Append a new value to the given key
 * - Remove Data by predicate: Drop any state using a predicate condition on keys or values
 *
 * @param joinSide              Defines the join side
 * @param inputValueAttributes  Attributes of the input row which will be stored as value
 * @param joinKeys              Expressions to generate rows that will be used to key the value rows
 * @param stateInfo             Information about how to retrieve the correct version of state
 * @param storeConf             Configuration for the state store.
 * @param hadoopConf            Hadoop configuration for reading state data from storage
 * @param partitionId           A partition ID of source RDD.
 * @param stateFormatVersion    The version of format for state.
 *
 * Internally, the key -> multiple values is stored in two [[StateStore]]s.
 * - Store 1 ([[KeyToNumValuesStore]]) maintains mapping between key -> number of values
 * - Store 2 ([[KeyWithIndexToValueStore]]) maintains mapping; the mapping depends on the state
 *   format version:
 *   - version 1: [(key, index) -> value]
 *   - version 2: [(key, index) -> (value, matched)]
 * - Put:   update count in KeyToNumValuesStore,
 *          insert new (key, count) -> value in KeyWithIndexToValueStore
 * - Get:   read count from KeyToNumValuesStore,
 *          read each of the n values in KeyWithIndexToValueStore
 * - Remove state by predicate on keys:
 *          scan all keys in KeyToNumValuesStore to find keys that do match the predicate,
 *          delete from key from KeyToNumValuesStore, delete values in KeyWithIndexToValueStore
 * - Remove state by condition on values:
 *          scan all elements in KeyWithIndexToValueStore to find values that match
 *          the predicate, delete corresponding (key, indexToDelete) from KeyWithIndexToValueStore
 *          by overwriting with the value of (key, maxIndex), and removing [(key, maxIndex),
 *          decrement corresponding num values in KeyToNumValuesStore
 */
class SymmetricHashJoinStateManager(
    val joinSide: JoinSide,
    inputValueAttributes: Seq[Attribute],
    joinKeys: Seq[Expression],
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration,
    partitionId: Int,
    stateFormatVersion: Int)
  extends StreamingStateManager(inputValueAttributes, joinKeys, stateInfo, storeConf, hadoopConf,
    partitionId, stateFormatVersion) {

  import StreamingStateManagerBase._

  /**
   * Get all the matched values for given join condition, with marking matched.
   * This method is designed to mark joined rows properly without exposing internal index of row.
   *
   * @param excludeRowsAlreadyMatched Do not join with rows already matched previously.
   *                                  This is used for right side of left semi join in
   *                                  [[StreamingSymmetricHashJoinExec]] only.
   */
  def getJoinedRows(
      key: UnsafeRow,
      generateJoinedRow: InternalRow => JoinedRow,
      predicate: JoinedRow => Boolean,
      excludeRowsAlreadyMatched: Boolean = false): Iterator[JoinedRow] = {
    val numValues = keyToNumValues.get(key)
    keyWithIndexToValue.getAll(key, numValues).filterNot { keyIdxToValue =>
      excludeRowsAlreadyMatched && keyIdxToValue.matched
    }.map { keyIdxToValue =>
      val joinedRow = generateJoinedRow(keyIdxToValue.value)
      if (predicate(joinedRow)) {
        if (!keyIdxToValue.matched) {
          keyWithIndexToValue.put(key, keyIdxToValue.valueIndex, keyIdxToValue.value,
            matched = true)
        }
        joinedRow
      } else {
        null
      }
    }.filter(_ != null)
  }

  /**
   * Remove using a predicate on keys.
   *
   * This produces an iterator over the (key, value, matched) tuples satisfying condition(key),
   * where the underlying store is updated as a side-effect of producing next.
   *
   * This implies the iterator must be consumed fully without any other operations on this manager
   * or the underlying store being interleaved.
   */
  def removeByKeyCondition(removalCondition: UnsafeRow => Boolean): Iterator[KeyToValuePair] = {
    new NextIterator[KeyToValuePair] {

      private val allKeyToNumValues = keyToNumValues.iterator

      private var currentKeyToNumValue: KeyAndNumValues = null
      private var currentValues: Iterator[KeyWithIndexAndValue] = null

      private def currentKey = currentKeyToNumValue.key

      private val reusedRet = new KeyToValuePair()

      private def getAndRemoveValue(): KeyToValuePair = {
        val keyWithIndexAndValue = currentValues.next()
        keyWithIndexToValue.remove(currentKey, keyWithIndexAndValue.valueIndex)
        reusedRet.withNew(currentKey, keyWithIndexAndValue.value, keyWithIndexAndValue.matched)
      }

      override def getNext(): KeyToValuePair = {
        // If there are more values for the current key, remove and return the next one.
        if (currentValues != null && currentValues.hasNext) {
          return getAndRemoveValue()
        }

        // If there weren't any values left, try and find the next key that satisfies the removal
        // condition and has values.
        while (allKeyToNumValues.hasNext) {
          currentKeyToNumValue = allKeyToNumValues.next()
          if (removalCondition(currentKey)) {
            currentValues = keyWithIndexToValue.getAll(currentKey, currentKeyToNumValue.numValue)
            keyToNumValues.remove(currentKey)

            if (currentValues.hasNext) {
              return getAndRemoveValue()
            }
          }
        }

        // We only reach here if there were no satisfying keys left, which means we're done.
        finished = true
        return null
      }

      override def close(): Unit = {}
    }
  }

  /**
   * Remove using a predicate on values.
   *
   * At a high level, this produces an iterator over the (key, value, matched) tuples such that
   * value satisfies the predicate, where producing an element removes the value from the
   * state store and producing all elements with a given key updates it accordingly.
   *
   * This implies the iterator must be consumed fully without any other operations on this manager
   * or the underlying store being interleaved.
   */
  def removeByValueCondition(removalCondition: UnsafeRow => Boolean): Iterator[KeyToValuePair] = {
    new NextIterator[KeyToValuePair] {

      // Reuse this object to avoid creation+GC overhead.
      private val reusedRet = new KeyToValuePair()

      private val allKeyToNumValues = keyToNumValues.iterator

      private var currentKey: UnsafeRow = null
      private var numValues: Long = 0L
      private var index: Long = 0L
      private var valueRemoved: Boolean = false

      // Push the data for the current key to the numValues store, and reset the tracking variables
      // to their empty state.
      private def updateNumValueForCurrentKey(): Unit = {
        if (valueRemoved) {
          if (numValues >= 1) {
            keyToNumValues.put(currentKey, numValues)
          } else {
            keyToNumValues.remove(currentKey)
          }
        }

        currentKey = null
        numValues = 0
        index = 0
        valueRemoved = false
      }

      // Find the next value satisfying the condition, updating `currentKey` and `numValues` if
      // needed. Returns null when no value can be found.
      private def findNextValueForIndex(): ValueAndMatchPair = {
        // Loop across all values for the current key, and then all other keys, until we find a
        // value satisfying the removal condition.
        def hasMoreValuesForCurrentKey = currentKey != null && index < numValues
        def hasMoreKeys = allKeyToNumValues.hasNext
        while (hasMoreValuesForCurrentKey || hasMoreKeys) {
          if (hasMoreValuesForCurrentKey) {
            // First search the values for the current key.
            val valuePair = keyWithIndexToValue.get(currentKey, index)
            if (removalCondition(valuePair.value)) {
              return valuePair
            } else {
              index += 1
            }
          } else if (hasMoreKeys) {
            // If we can't find a value for the current key, cleanup and start looking at the next.
            // This will also happen the first time the iterator is called.
            updateNumValueForCurrentKey()

            val currentKeyToNumValue = allKeyToNumValues.next()
            currentKey = currentKeyToNumValue.key
            numValues = currentKeyToNumValue.numValue
          } else {
            // Should be unreachable, but in any case means a value couldn't be found.
            return null
          }
        }

        // We tried and failed to find the next value.
        return null
      }

      override def getNext(): KeyToValuePair = {
        val currentValue = findNextValueForIndex()

        // If there's no value, clean up and finish. There aren't any more available.
        if (currentValue == null) {
          updateNumValueForCurrentKey()
          finished = true
          return null
        }

        // The backing store is arraylike - we as the caller are responsible for filling back in
        // any hole. So we swap the last element into the hole and decrement numValues to shorten.
        // clean
        if (numValues > 1) {
          val valuePairAtMaxIndex = keyWithIndexToValue.get(currentKey, numValues - 1)
          if (valuePairAtMaxIndex != null) {
            keyWithIndexToValue.put(currentKey, index, valuePairAtMaxIndex.value,
              valuePairAtMaxIndex.matched)
          } else {
            keyWithIndexToValue.put(currentKey, index, null, false)
          }
          keyWithIndexToValue.remove(currentKey, numValues - 1)
        } else {
          keyWithIndexToValue.remove(currentKey, 0)
        }
        numValues -= 1
        valueRemoved = true

        return reusedRet.withNew(currentKey, currentValue.value, currentValue.matched)
      }

      override def close(): Unit = {}
    }
  }

  /** Get the combined metrics of all the state stores */
  override def metrics: StateStoreMetrics = {
    val keyToNumValuesMetrics = keyToNumValues.metrics
    val keyWithIndexToValueMetrics = keyWithIndexToValue.metrics
    def newDesc(desc: String): String = s"${joinSide.toString.toUpperCase(Locale.ROOT)}: $desc"

    StateStoreMetrics(
      keyWithIndexToValueMetrics.numKeys,       // represent each buffered row only once
      keyToNumValuesMetrics.memoryUsedBytes + keyWithIndexToValueMetrics.memoryUsedBytes,
      keyWithIndexToValueMetrics.customMetrics.map {
        case (s @ StateStoreCustomSumMetric(_, desc), value) =>
          s.copy(desc = newDesc(desc)) -> value
        case (s @ StateStoreCustomSizeMetric(_, desc), value) =>
          s.copy(desc = newDesc(desc)) -> value
        case (s @ StateStoreCustomTimingMetric(_, desc), value) =>
          s.copy(desc = newDesc(desc)) -> value
        case (s, _) =>
          throw new IllegalArgumentException(
            s"Unknown state store custom metric is found at metrics: $s")
      }
    )
  }

  override protected def getStateStoreName(storeType: StateStoreType): String = {
    s"$joinSide-$storeType"
  }
}

object SymmetricHashJoinStateManager {
  import StreamingStateManagerBase._

  val supportedVersions = Seq(1, 2)
  val legacyVersion = 1

  def allStateStoreNames(joinSides: JoinSide*): Seq[String] = {
    val allStateStoreTypes: Seq[StateStoreType] = Seq(KeyToNumValuesType, KeyWithIndexToValueType)
    for (joinSide <- joinSides; stateStoreType <- allStateStoreTypes) yield {
      s"$joinSide-$stateStoreType"
    }
  }
}
