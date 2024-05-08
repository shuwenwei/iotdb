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
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.ModifiedStatus;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.group.chunk.CompactedChunkRecord;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.group.chunk.GroupCompactionAlignedPagePointReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.group.chunk.NonFirstGroupAlignedChunkWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.ReadChunkAlignedSeriesCompactionExecutor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader.ChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader.PageLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IDeviceID;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.page.AlignedPageReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class NonFirstAlignedSeriesGroupCompactionExecutor
    extends ReadChunkAlignedSeriesCompactionExecutor {
  private final List<CompactedChunkRecord> compactionPlan;
  private int currentCompactChunk;

  public NonFirstAlignedSeriesGroupCompactionExecutor(
      IDeviceID device,
      TsFileResource targetResource,
      LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList,
      CompactionTsFileWriter writer,
      CompactionTaskSummary summary,
      IMeasurementSchema timeSchema,
      List<IMeasurementSchema> valueSchemaList,
      List<CompactedChunkRecord> compactionPlan) {
    super(
        device,
        targetResource,
        readerAndChunkMetadataList,
        writer,
        summary,
        timeSchema,
        valueSchemaList);
    this.compactionPlan = compactionPlan;
    this.flushPolicy = new ColumnGroupFlushDataBlockPolicy();
    this.chunkWriter =
        new NonFirstGroupAlignedChunkWriter(timeSchema, schemaList, compactionPlan.get(0));
  }

  @Override
  protected void flushCurrentChunkWriter() throws IOException {
    if (chunkWriter.isEmpty()) {
      return;
    }
    super.flushCurrentChunkWriter();
    currentCompactChunk++;
    if (currentCompactChunk < compactionPlan.size()) {
      CompactedChunkRecord chunkRecord = compactionPlan.get(currentCompactChunk);
      this.chunkWriter = new NonFirstGroupAlignedChunkWriter(timeSchema, schemaList, chunkRecord);
    }
  }

  @Override
  protected void compactAlignedChunkByFlush(ChunkLoader timeChunk, List<ChunkLoader> valueChunks)
      throws IOException {
    writer.markStartingWritingAligned();
    checkAndUpdatePreviousTimestamp(timeChunk.getChunkMetadata().getStartTime());
    checkAndUpdatePreviousTimestamp(timeChunk.getChunkMetadata().getEndTime());
    timeChunk.clear();
    int nonEmptyChunkNum = 0;
    for (int i = 0; i < valueChunks.size(); i++) {
      ChunkLoader valueChunk = valueChunks.get(i);
      if (valueChunk.isEmpty()) {
        IMeasurementSchema schema = schemaList.get(i);
        writer.writeEmptyValueChunk(
            schema.getMeasurementId(),
            schema.getCompressor(),
            schema.getType(),
            schema.getEncodingType(),
            Statistics.getStatsByType(schema.getType()));
        continue;
      }
      nonEmptyChunkNum++;
      writer.writeChunk(valueChunk.getChunk(), valueChunk.getChunkMetadata());
      valueChunk.clear();
    }
    summary.increaseDirectlyFlushChunkNum(nonEmptyChunkNum);
    writer.markEndingWritingAligned();
    currentCompactChunk++;
  }

  @Override
  protected IPointReader getPointReader(AlignedPageReader alignedPageReader) throws IOException {
    return new GroupCompactionAlignedPagePointReader(
        alignedPageReader.getTimePageReader(), alignedPageReader.getValuePageReaderList());
  }

  private class ColumnGroupFlushDataBlockPolicy extends FlushDataBlockPolicy {

    public ColumnGroupFlushDataBlockPolicy() {
      super(0);
    }

    @Override
    public boolean canCompactCurrentChunkByDirectlyFlush(
        ChunkLoader timeChunk, List<ChunkLoader> valueChunks) throws IOException {
      return compactionPlan.get(currentCompactChunk).isCompactedByDirectlyFlush();
    }

    @Override
    protected boolean canFlushCurrentChunkWriter() {
      return chunkWriter.checkIsChunkSizeOverThreshold(0, 0, true);
    }

    @Override
    protected boolean canCompactCurrentPageByDirectlyFlush(
        PageLoader timePage, List<PageLoader> valuePages) {
      int currentPage = ((NonFirstGroupAlignedChunkWriter) chunkWriter).getCurrentPage();
      for (int i = 0; i < valuePages.size(); i++) {
        PageLoader currentValuePage = valuePages.get(i);
        if (currentValuePage.isEmpty()) {
          continue;
        }
        if (currentValuePage.getCompressionType() != schemaList.get(i).getCompressor()
            || currentValuePage.getEncoding() != schemaList.get(i).getEncodingType()) {
          return false;
        }
        if (currentValuePage.getModifiedStatus() == ModifiedStatus.PARTIAL_DELETED) {
          return false;
        }
      }
      return compactionPlan
          .get(currentCompactChunk)
          .getPageRecords()
          .get(currentPage)
          .isCompactedByDirectlyFlush();
    }
  }
}
