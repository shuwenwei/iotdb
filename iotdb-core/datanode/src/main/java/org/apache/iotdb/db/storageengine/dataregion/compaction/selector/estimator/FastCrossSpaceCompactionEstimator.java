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

package org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator;

import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class FastCrossSpaceCompactionEstimator extends AbstractCrossSpaceEstimator {

  @Override
  protected long calculatingMetadataMemoryCost(CompactionTaskInfo taskInfo) {
    long cost = 0;
    // add ChunkMetadata size of MultiTsFileDeviceIterator
    cost +=
        Math.min(
            taskInfo.getTotalChunkMetadataSize(),
            taskInfo.getFileInfoList().size()
                * taskInfo.getMaxChunkMetadataNumInDevice()
                * taskInfo.getMaxChunkMetadataSize());

    // add ChunkMetadata size of targetFileWriter
    cost += fixedMemoryBudget;

    return cost;
  }

  @Override
  protected long calculatingDataMemoryCost(CompactionTaskInfo taskInfo) throws IOException {
    if (taskInfo.getTotalChunkNum() == 0) {
      return taskInfo.getModificationFileSize();
    }

    int batchSize = config.getCompactionMaxAlignedSeriesNumInOneBatch();
    long maxConcurrentSeriesNum =
        Math.max(
            config.getSubCompactionTaskNum(),
            Math.min(
                batchSize <= 0 ? Integer.MAX_VALUE : batchSize,
                taskInfo.getMaxConcurrentSeriesNum()));
    long averageChunkSize = taskInfo.getTotalFileSize() / taskInfo.getTotalChunkNum();

    long maxConcurrentSeriesSizeOfTotalFiles =
        averageChunkSize
                * taskInfo.getFileInfoList().size()
                * maxConcurrentSeriesNum
                * taskInfo.getMaxChunkMetadataNumInSeries()
            + maxConcurrentSeriesNum * tsFileConfig.getPageSizeInByte();
    long maxTargetChunkWriterSize = config.getTargetChunkSize() * maxConcurrentSeriesNum;
    long targetChunkWriterSize =
        Math.min(maxConcurrentSeriesSizeOfTotalFiles, maxTargetChunkWriterSize);

    long maxConcurrentChunkSizeFromSourceFile =
        (averageChunkSize + tsFileConfig.getPageSizeInByte())
            * maxConcurrentSeriesNum
            * calculatingMaxOverlapFileNumInSubCompactionTask(taskInfo.getResources());

    return targetChunkWriterSize
        + maxConcurrentChunkSizeFromSourceFile
        + taskInfo.getModificationFileSize();
  }

  @Override
  public long roughEstimateCrossCompactionMemory(
      List<TsFileResource> seqResources, List<TsFileResource> unseqResources) throws IOException {
    if (config.getCompactionMaxAlignedSeriesNumInOneBatch() <= 0) {
      return -1L;
    }
    List<TsFileResource> sourceFiles = new ArrayList<>(seqResources.size() + unseqResources.size());
    sourceFiles.addAll(seqResources);
    sourceFiles.addAll(unseqResources);

    Optional<MetadataInfo> metadataInfo =
        CompactionEstimateUtils.collectMetadataInfo(sourceFiles, CompactionType.CROSS_COMPACTION);
    if (!metadataInfo.isPresent()) {
      return -1L;
    }

    int maxConcurrentSeriesNum = metadataInfo.get().getMaxConcurrentSeriesNum();
    long maxChunkSize = config.getTargetChunkSize();
    long maxPageSize = tsFileConfig.getPageSizeInByte();
    int maxOverlapFileNum = calculatingMaxOverlapFileNumInSubCompactionTask(sourceFiles);
    // source files (chunk + uncompressed page) * overlap file num
    // target files (chunk + unsealed page writer)
    return (maxOverlapFileNum + 1) * maxConcurrentSeriesNum * (maxChunkSize + maxPageSize)
        + fixedMemoryBudget
        + metadataInfo.get().metadataMemCost;
  }
}
