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

package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * A vectorized Spark Orc RecordReader.
 * This is based on hive-exec-1.2.1
 * {@link org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat.OrcRecordReader}.
 * This class exposes getObjectInspector which can be used for reducing
 * NameNode calls in OrcRelation.
 */
public class VectorizedSparkOrcNewRecordReader extends
    org.apache.hadoop.mapreduce.RecordReader<NullWritable, OrcStruct> {
  private final org.apache.hadoop.hive.ql.io.orc.RecordReader reader;
  private final int numColumns;
  OrcStruct value;
  private float progress = 0.0f;
  private ObjectInspector objectInspector;

  /**
   * If true, this class returns batches instead of rows.
   */
  private boolean returnColumnarBatch;

  private VectorizedRowBatch columnarBatch;
  private ColumnVector[] internalColumns;

  /**
   * Batch of rows that we assemble and the current index we've returned. Every time this
   * batch is used up (batchIdx == numBatched), we populated the batch.
   */
  private int batchIdx = 0;
  private int numBatched = 0;

  public VectorizedSparkOrcNewRecordReader(Reader file, Configuration conf,
      long offset, long length) throws IOException {
    List<OrcProto.Type> types = file.getTypes();
    numColumns = (types.size() == 0) ? 0 : types.get(0).getSubtypesCount();
    value = new OrcStruct(numColumns);
    this.reader = OrcInputFormat.createReaderFromFile(file, conf, offset,
        length);
    this.objectInspector = file.getObjectInspector();

    this.columnarBatch = new VectorizedRowBatch(numColumns);
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public NullWritable getCurrentKey() throws IOException,
      InterruptedException {
    return NullWritable.get();
  }

  @Override
  public OrcStruct getCurrentValue() throws IOException,
      InterruptedException {
    if (returnColumnarBatch) return columnarBatch;
    return columnarBatch.getRow(batchIdx - 1);
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return progress;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
  }

  /**
   * Advances to the next batch of rows. Returns false if there are no more.
   */
  public boolean nextBatch() throws IOException {
    columnarBatch.reset();
    columnarBatch = reader.nextBatch(columnarBatch);
    if (columnarBatch.endOfFile) return false;

    internalColumns = columnarBatch.cols;
    numBatched = columnarBatch.count();
    batchIdx = 0;
    return true;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (reader.hasNext()) {
      reader.next(value);
      progress = reader.getProgress();
      return true;
    } else {
      return false;
    }
  }

  public ObjectInspector getObjectInspector() {
    return objectInspector;
  }
}
