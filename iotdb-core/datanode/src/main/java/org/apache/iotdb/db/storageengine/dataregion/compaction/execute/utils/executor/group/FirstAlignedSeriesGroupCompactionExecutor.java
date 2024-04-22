/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.group;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.group.chunk.CompactedChunkRecord;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.group.chunk.FirstGroupAlignedChunkWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.ReadChunkAlignedSeriesCompactionExecutor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader.ChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IDeviceID;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class FirstAlignedSeriesGroupCompactionExecutor
    extends ReadChunkAlignedSeriesCompactionExecutor {

  private final List<CompactedChunkRecord> compactedChunkRecords = new ArrayList<>();

  public FirstAlignedSeriesGroupCompactionExecutor(
      IDeviceID device,
      TsFileResource targetResource,
      LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList,
      CompactionTsFileWriter writer,
      CompactionTaskSummary summary,
      IMeasurementSchema timeSchema,
      List<IMeasurementSchema> valueSchemaList) {
    super(
        device,
        targetResource,
        readerAndChunkMetadataList,
        writer,
        summary,
        timeSchema,
        valueSchemaList);
  }

  @Override
  protected AlignedChunkWriterImpl constructAlignedChunkWriter() {
    return new FirstGroupAlignedChunkWriter(timeSchema, schemaList);
  }

  @Override
  protected void compactAlignedChunkByFlush(ChunkLoader timeChunk, List<ChunkLoader> valueChunks)
      throws IOException {
    ChunkMetadata timeChunkMetadata = timeChunk.getChunkMetadata();
    compactedChunkRecords.add(
        new CompactedChunkRecord(timeChunkMetadata.getStartTime(), timeChunkMetadata.getEndTime()));
    super.compactAlignedChunkByFlush(timeChunk, valueChunks);
  }

  @Override
  protected void flushCurrentChunkWriter() throws IOException {
    chunkWriter.sealCurrentPage();
    if (!chunkWriter.isEmpty()) {
      CompactedChunkRecord compactedChunkRecord =
          ((FirstGroupAlignedChunkWriter) chunkWriter).getCompactedChunkRecord();
      compactedChunkRecords.add(compactedChunkRecord);
    }
    writer.writeChunk(chunkWriter);
  }

  public List<CompactedChunkRecord> getCompactedChunkRecords() {
    return compactedChunkRecords;
  }
}
