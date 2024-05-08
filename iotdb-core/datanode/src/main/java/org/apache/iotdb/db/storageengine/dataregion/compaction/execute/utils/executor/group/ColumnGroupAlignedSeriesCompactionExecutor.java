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
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.ReadChunkAlignedSeriesCompactionExecutor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IDeviceID;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;
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

public class ColumnGroupAlignedSeriesCompactionExecutor
    extends ReadChunkAlignedSeriesCompactionExecutor {

  private int compactColumnNum = 2;
  private Set<String> compactedMeasurements;

  public ColumnGroupAlignedSeriesCompactionExecutor(
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
    List<CompactedChunkRecord> compactedChunkRecords = compactFirstColumnGroup();
    compactLeftColumnGroups(compactedChunkRecords);
  }

  private List<CompactedChunkRecord> compactFirstColumnGroup() throws IOException, PageException {
    List<IMeasurementSchema> firstGroupMeasurements = selectColumnGroupToCompact();

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
    for (CompactedChunkRecord compactedChunkRecord : executor.getCompactedChunkRecords()) {
      System.out.println(compactedChunkRecord);
    }
    return executor.getCompactedChunkRecords();
  }

  private void compactLeftColumnGroups(List<CompactedChunkRecord> compactedChunkRecords)
      throws PageException, IOException {
    while (compactedMeasurements.size() < schemaList.size()) {
      List<IMeasurementSchema> selectedColumnGroup = selectColumnGroupToCompact();
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
              compactedChunkRecords);
      executor.execute();
    }
  }

  private List<IMeasurementSchema> selectColumnGroupToCompact() {
    List<IMeasurementSchema> selectedColumnGroup = new ArrayList<>(compactColumnNum);
    for (IMeasurementSchema schema : schemaList) {
      if (!schema.getType().equals(TSDataType.TEXT)) {
        continue;
      }
      if (compactedMeasurements.contains(schema.getMeasurementId())) {
        continue;
      }
      if (!compactedMeasurements.contains(schema.getMeasurementId())
          && schema.getType().equals(TSDataType.TEXT)) {
        compactedMeasurements.add(schema.getMeasurementId());
        selectedColumnGroup.add(schema);
      }
      selectedColumnGroup.add(schema);
      compactedMeasurements.add(schema.getMeasurementId());
      if (compactedMeasurements.size() == schemaList.size()) {
        return selectedColumnGroup;
      }
    }
    for (IMeasurementSchema schema : schemaList) {
      if (compactedMeasurements.contains(schema.getMeasurementId())) {
        continue;
      }
      selectedColumnGroup.add(schema);
      compactedMeasurements.add(schema.getMeasurementId());
      if (selectedColumnGroup.size() == compactColumnNum) {
        break;
      }
      if (compactedMeasurements.size() == schemaList.size()) {
        break;
      }
    }
    return selectedColumnGroup;
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
      if (measurementIndex.containsKey(chunkMetadata.getMeasurementUid())) {
        valueChunkMetadataList.set(
            measurementIndex.get(chunkMetadata.getMeasurementUid()), chunkMetadata);
      }
    }
    return new AlignedChunkMetadata(
        alignedChunkMetadata.getTimeChunkMetadata(), valueChunkMetadataList);
  }
}
