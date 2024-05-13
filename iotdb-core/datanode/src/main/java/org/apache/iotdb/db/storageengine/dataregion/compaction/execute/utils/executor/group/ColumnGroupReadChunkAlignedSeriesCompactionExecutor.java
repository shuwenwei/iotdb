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
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.group.chunk.CompactChunkPlan;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.group.chunk.FirstGroupAlignedChunkWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.group.chunk.GroupCompactionAlignedPagePointReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.group.chunk.NonFirstGroupAlignedChunkWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.group.util.AlignedSeriesGroupCompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.ReadChunkAlignedSeriesCompactionExecutor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader.ChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader.PageLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IDeviceID;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.page.AlignedPageReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ColumnGroupReadChunkAlignedSeriesCompactionExecutor
    extends ReadChunkAlignedSeriesCompactionExecutor {

  private final Set<String> compactedMeasurements;

  public ColumnGroupReadChunkAlignedSeriesCompactionExecutor(
      IDeviceID device,
      TsFileResource targetResource,
      LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList,
      CompactionTsFileWriter writer,
      CompactionTaskSummary summary)
      throws IOException {
    super(device, targetResource, readerAndChunkMetadataList, writer, summary);
    compactedMeasurements = new HashSet<>();
  }

  @Override
  public void execute() throws IOException, PageException {
    markAlignedChunkHasDeletion(readerAndChunkMetadataList);
    List<CompactChunkPlan> compactChunkPlans = compactFirstColumnGroup();
    compactLeftColumnGroups(compactChunkPlans);
  }

  private void markAlignedChunkHasDeletion(
      LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
          readerAndChunkMetadataList) {
    for (Pair<TsFileSequenceReader, List<AlignedChunkMetadata>> pair : readerAndChunkMetadataList) {
      List<AlignedChunkMetadata> alignedChunkMetadataList = pair.getRight();
      for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadataList) {
        IChunkMetadata timeChunkMetadata = alignedChunkMetadata.getTimeChunkMetadata();
        for (IChunkMetadata iChunkMetadata : alignedChunkMetadata.getValueChunkMetadataList()) {
          if (iChunkMetadata != null && iChunkMetadata.isModified()) {
            timeChunkMetadata.setModified(true);
            break;
          }
        }
      }
    }
  }

  private List<CompactChunkPlan> compactFirstColumnGroup() throws IOException, PageException {
    List<IMeasurementSchema> firstGroupMeasurements =
        AlignedSeriesGroupCompactionUtils.selectColumnGroupToCompact(
            schemaList, compactedMeasurements);

    LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
        groupReaderAndChunkMetadataList =
            filterAlignedChunkMetadataList(
                readerAndChunkMetadataList,
                firstGroupMeasurements.stream()
                    .map(IMeasurementSchema::getMeasurementId)
                    .collect(Collectors.toList()));
    FirstAlignedSeriesGroupCompactionExecutor executor =
        new FirstAlignedSeriesGroupCompactionExecutor(
            device,
            targetResource,
            groupReaderAndChunkMetadataList,
            writer,
            summary,
            timeSchema,
            firstGroupMeasurements);
    executor.execute();
    return executor.getCompactedChunkRecords();
  }

  private void compactLeftColumnGroups(List<CompactChunkPlan> compactChunkPlans)
      throws PageException, IOException {
    while (compactedMeasurements.size() < schemaList.size()) {
      List<IMeasurementSchema> selectedColumnGroup =
          AlignedSeriesGroupCompactionUtils.selectColumnGroupToCompact(
              schemaList, compactedMeasurements);
      LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
          groupReaderAndChunkMetadataList =
              filterAlignedChunkMetadataList(
                  readerAndChunkMetadataList,
                  selectedColumnGroup.stream()
                      .map(IMeasurementSchema::getMeasurementId)
                      .collect(Collectors.toList()));
      NonFirstAlignedSeriesGroupCompactionExecutor executor =
          new NonFirstAlignedSeriesGroupCompactionExecutor(
              device,
              targetResource,
              groupReaderAndChunkMetadataList,
              writer,
              summary,
              timeSchema,
              selectedColumnGroup,
              compactChunkPlans);
      executor.execute();
    }
  }

  private LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
      filterAlignedChunkMetadataList(
          List<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList,
          List<String> selectedMeasurements) {
    LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
        groupReaderAndChunkMetadataList = new LinkedList<>();
    for (Pair<TsFileSequenceReader, List<AlignedChunkMetadata>> pair : readerAndChunkMetadataList) {
      List<AlignedChunkMetadata> alignedChunkMetadataList = pair.getRight();
      List<AlignedChunkMetadata> selectedColumnAlignedChunkMetadataList = new LinkedList<>();
      for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadataList) {
        selectedColumnAlignedChunkMetadataList.add(
            filterAlignedChunkMetadata(alignedChunkMetadata, selectedMeasurements));
      }
      groupReaderAndChunkMetadataList.add(
          new Pair<>(pair.getLeft(), selectedColumnAlignedChunkMetadataList));
    }
    return groupReaderAndChunkMetadataList;
  }

  private AlignedChunkMetadata filterAlignedChunkMetadata(
      AlignedChunkMetadata alignedChunkMetadata, List<String> selectedMeasurements) {
    List<IChunkMetadata> valueChunkMetadataList =
        Arrays.asList(new IChunkMetadata[selectedMeasurements.size()]);

    Map<String, Integer> measurementIndex = new HashMap<>();
    for (int i = 0; i < selectedMeasurements.size(); i++) {
      measurementIndex.put(selectedMeasurements.get(i), i);
    }

    for (IChunkMetadata chunkMetadata : alignedChunkMetadata.getValueChunkMetadataList()) {
      if (chunkMetadata == null) {
        continue;
      }
      if (measurementIndex.containsKey(chunkMetadata.getMeasurementUid())) {
        valueChunkMetadataList.set(
            measurementIndex.get(chunkMetadata.getMeasurementUid()), chunkMetadata);
      }
    }
    return new AlignedChunkMetadata(
        alignedChunkMetadata.getTimeChunkMetadata(), valueChunkMetadataList);
  }

  public static class FirstAlignedSeriesGroupCompactionExecutor
      extends ReadChunkAlignedSeriesCompactionExecutor {

    private final List<CompactChunkPlan> compactChunkPlans = new ArrayList<>();

    public FirstAlignedSeriesGroupCompactionExecutor(
        IDeviceID device,
        TsFileResource targetResource,
        LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
            readerAndChunkMetadataList,
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
      int compactionFileLevel =
          Integer.parseInt(this.targetResource.getTsFile().getName().split("-")[2]);
      this.flushPolicy = new FirstColumnGroupFlushDataBlockPolicy(compactionFileLevel);
    }

    @Override
    protected AlignedChunkWriterImpl constructAlignedChunkWriter() {
      return new FirstGroupAlignedChunkWriter(timeSchema, schemaList);
    }

    @Override
    protected void compactAlignedChunkByFlush(ChunkLoader timeChunk, List<ChunkLoader> valueChunks)
        throws IOException {
      ChunkMetadata timeChunkMetadata = timeChunk.getChunkMetadata();
      compactChunkPlans.add(
          new CompactChunkPlan(timeChunkMetadata.getStartTime(), timeChunkMetadata.getEndTime()));
      super.compactAlignedChunkByFlush(timeChunk, valueChunks);
    }

    @Override
    protected void flushCurrentChunkWriter() throws IOException {
      chunkWriter.sealCurrentPage();
      if (!chunkWriter.isEmpty()) {
        CompactChunkPlan compactChunkPlan =
            ((FirstGroupAlignedChunkWriter) chunkWriter).getCompactedChunkRecord();
        compactChunkPlans.add(compactChunkPlan);
      }
      writer.writeChunk(chunkWriter);
    }

    public List<CompactChunkPlan> getCompactedChunkRecords() {
      return compactChunkPlans;
    }

    @Override
    protected IPointReader getPointReader(AlignedPageReader alignedPageReader) throws IOException {
      return new GroupCompactionAlignedPagePointReader(
          alignedPageReader.getTimePageReader(), alignedPageReader.getValuePageReaderList());
    }

    private class FirstColumnGroupFlushDataBlockPolicy extends FlushDataBlockPolicy {

      public FirstColumnGroupFlushDataBlockPolicy(int compactionFileLevel) {
        super(compactionFileLevel);
      }

      @Override
      public boolean canCompactCurrentChunkByDirectlyFlush(
          ChunkLoader timeChunk, List<ChunkLoader> valueChunks) throws IOException {
        return !timeChunk.getChunkMetadata().isModified()
            && super.canCompactCurrentChunkByDirectlyFlush(timeChunk, valueChunks);
      }
    }
  }

  public static class NonFirstAlignedSeriesGroupCompactionExecutor
      extends ReadChunkAlignedSeriesCompactionExecutor {
    private final List<CompactChunkPlan> compactionPlan;
    private int currentCompactChunk;

    public NonFirstAlignedSeriesGroupCompactionExecutor(
        IDeviceID device,
        TsFileResource targetResource,
        LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
            readerAndChunkMetadataList,
        CompactionTsFileWriter writer,
        CompactionTaskSummary summary,
        IMeasurementSchema timeSchema,
        List<IMeasurementSchema> valueSchemaList,
        List<CompactChunkPlan> compactionPlan) {
      super(
          device,
          targetResource,
          readerAndChunkMetadataList,
          writer,
          summary,
          timeSchema,
          valueSchemaList);
      this.compactionPlan = compactionPlan;
      this.flushPolicy = new NonFirstColumnGroupFlushDataBlockPolicy();
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
        CompactChunkPlan chunkRecord = compactionPlan.get(currentCompactChunk);
        ((NonFirstGroupAlignedChunkWriter) this.chunkWriter).setCompactChunkPlan(chunkRecord);
      }
    }

    @Override
    protected void compactAlignedChunkByFlush(ChunkLoader timeChunk, List<ChunkLoader> valueChunks)
        throws IOException {
      writer.markStartingWritingAligned();
      checkAndUpdatePreviousTimestamp(timeChunk.getChunkMetadata().getStartTime());
      checkAndUpdatePreviousTimestamp(timeChunk.getChunkMetadata().getEndTime());
      // skip time chunk
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
    protected void compactAlignedPageByFlush(PageLoader timePage, List<PageLoader> valuePageLoaders)
        throws PageException, IOException {
      int nonEmptyPage = 0;
      checkAndUpdatePreviousTimestamp(timePage.getHeader().getStartTime());
      checkAndUpdatePreviousTimestamp(timePage.getHeader().getEndTime());
      // skip time page
      timePage.clear();
      for (int i = 0; i < valuePageLoaders.size(); i++) {
        PageLoader valuePage = valuePageLoaders.get(i);
        if (!valuePage.isEmpty()) {
          nonEmptyPage++;
        }
        valuePage.flushToValueChunkWriter(chunkWriter, i);
      }
      summary.increaseDirectlyFlushPageNum(nonEmptyPage);
    }

    @Override
    protected IPointReader getPointReader(AlignedPageReader alignedPageReader) throws IOException {
      return new GroupCompactionAlignedPagePointReader(
          alignedPageReader.getTimePageReader(), alignedPageReader.getValuePageReaderList());
    }

    private class NonFirstColumnGroupFlushDataBlockPolicy extends FlushDataBlockPolicy {

      public NonFirstColumnGroupFlushDataBlockPolicy() {
        super(0);
      }

      @Override
      public boolean canCompactCurrentChunkByDirectlyFlush(
          ChunkLoader timeChunk, List<ChunkLoader> valueChunks) throws IOException {
        return compactionPlan.get(currentCompactChunk).isCompactedByDirectlyFlush();
      }

      @Override
      protected boolean canFlushCurrentChunkWriter() {
        // the parameters are not used in this implementation
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
}