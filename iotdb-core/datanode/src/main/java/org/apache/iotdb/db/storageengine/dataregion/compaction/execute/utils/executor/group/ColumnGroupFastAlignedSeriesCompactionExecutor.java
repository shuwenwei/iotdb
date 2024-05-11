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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.AlignedSeriesCompactionExecutor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.group.chunk.CompactChunkPlan;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.group.util.AlignedSeriesGroupCompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.metadata.IDeviceID;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ColumnGroupFastAlignedSeriesCompactionExecutor
    extends AlignedSeriesCompactionExecutor {

  private final Set<String> compactedMeasurements;
  private final List<TsFileResource> sortedSourceFiles;

  public ColumnGroupFastAlignedSeriesCompactionExecutor(
      AbstractCompactionWriter compactionWriter,
      Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
      Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
      Map<TsFileResource, List<Modification>> modificationCacheMap,
      List<TsFileResource> sortedSourceFiles,
      IDeviceID deviceId,
      int subTaskId,
      List<IMeasurementSchema> measurementSchemas,
      FastCompactionTaskSummary summary) {
    super(
        compactionWriter,
        timeseriesMetadataOffsetMap,
        readerCacheMap,
        modificationCacheMap,
        sortedSourceFiles,
        deviceId,
        subTaskId,
        measurementSchemas,
        summary);
    this.sortedSourceFiles = sortedSourceFiles;
    this.compactedMeasurements = new HashSet<>();
  }

  @Override
  public void execute()
      throws PageException, IllegalPathException, IOException, WriteProcessException {
    List<CompactChunkPlan> compactChunkPlans = compactFirstColumnGroup();
    compactLeftColumnGroups(compactChunkPlans);
  }

  private List<CompactChunkPlan> compactFirstColumnGroup()
      throws IOException, PageException, IllegalPathException, WriteProcessException {
    List<IMeasurementSchema> firstGroupMeasurements =
        AlignedSeriesGroupCompactionUtils.selectColumnGroupToCompact(
            measurementSchemas, compactedMeasurements);
    Map<String, Map<TsFileResource, Pair<Long, Long>>>
        timeseriesMetadataOffsetMapOfSelectedColumnGroup =
            filterTimeseriesOffsetMap(timeseriesMetadataOffsetMap, firstGroupMeasurements);

    FirstAlignedSeriesGroupCompactionExecutor executor =
        new FirstAlignedSeriesGroupCompactionExecutor(
            compactionWriter,
            timeseriesMetadataOffsetMapOfSelectedColumnGroup,
            readerCacheMap,
            modificationCacheMap,
            sortedSourceFiles,
            deviceId,
            subTaskId,
            firstGroupMeasurements,
            summary);
    executor.execute();
    return executor.getCompactedChunkRecords();
  }

  private void compactLeftColumnGroups(List<CompactChunkPlan> compactChunkPlans)
      throws PageException, IOException, IllegalPathException, WriteProcessException {
    while (compactedMeasurements.size() < measurementSchemas.size()) {
      List<IMeasurementSchema> selectedColumnGroup =
          AlignedSeriesGroupCompactionUtils.selectColumnGroupToCompact(
              measurementSchemas, compactedMeasurements);
      Map<String, Map<TsFileResource, Pair<Long, Long>>>
          timeseriesMetadataOffsetMapOfSelectedColumnGroup =
              filterTimeseriesOffsetMap(timeseriesMetadataOffsetMap, selectedColumnGroup);
      NonFirstAlignedSeriesGroupCompactionExecutor executor =
          new NonFirstAlignedSeriesGroupCompactionExecutor(
              compactionWriter,
              timeseriesMetadataOffsetMapOfSelectedColumnGroup,
              readerCacheMap,
              modificationCacheMap,
              sortedSourceFiles,
              deviceId,
              subTaskId,
              selectedColumnGroup,
              summary,
              compactChunkPlans);
      executor.execute();
    }
  }

  private Map<String, Map<TsFileResource, Pair<Long, Long>>> filterTimeseriesOffsetMap(
      Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesOffsetMap,
      List<IMeasurementSchema> selectedMeasurements) {
    Map<String, Map<TsFileResource, Pair<Long, Long>>>
        timeseriesMetadataOffsetMapOfSelectedColumnGroup = new HashMap<>();
    for (IMeasurementSchema measurementSchema : selectedMeasurements) {
      String measurementID = measurementSchema.getMeasurementId();
      timeseriesMetadataOffsetMapOfSelectedColumnGroup.put(
          measurementID, timeseriesMetadataOffsetMap.get(measurementID));
    }
    return timeseriesMetadataOffsetMapOfSelectedColumnGroup;
  }

  private static class FirstAlignedSeriesGroupCompactionExecutor
      extends AlignedSeriesCompactionExecutor {

    private final List<CompactChunkPlan> compactChunkPlans = new ArrayList<>();

    public FirstAlignedSeriesGroupCompactionExecutor(
        AbstractCompactionWriter compactionWriter,
        Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
        Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
        Map<TsFileResource, List<Modification>> modificationCacheMap,
        List<TsFileResource> sortedSourceFiles,
        IDeviceID deviceId,
        int subTaskId,
        List<IMeasurementSchema> measurementSchemas,
        FastCompactionTaskSummary summary) {
      super(
          compactionWriter,
          timeseriesMetadataOffsetMap,
          readerCacheMap,
          modificationCacheMap,
          sortedSourceFiles,
          deviceId,
          subTaskId,
          measurementSchemas,
          summary);
    }

    public List<CompactChunkPlan> getCompactedChunkRecords() {
      return compactChunkPlans;
    }
  }

  private static class NonFirstAlignedSeriesGroupCompactionExecutor
      extends AlignedSeriesCompactionExecutor {

    public NonFirstAlignedSeriesGroupCompactionExecutor(
        AbstractCompactionWriter compactionWriter,
        Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
        Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
        Map<TsFileResource, List<Modification>> modificationCacheMap,
        List<TsFileResource> sortedSourceFiles,
        IDeviceID deviceId,
        int subTaskId,
        List<IMeasurementSchema> measurementSchemas,
        FastCompactionTaskSummary summary,
        List<CompactChunkPlan> compactionPlan) {
      super(
          compactionWriter,
          timeseriesMetadataOffsetMap,
          readerCacheMap,
          modificationCacheMap,
          sortedSourceFiles,
          deviceId,
          subTaskId,
          measurementSchemas,
          summary);
    }
  }
}
