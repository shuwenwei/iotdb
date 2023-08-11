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

package org.apache.iotdb.db.storageengine.dataregion.compaction.tool.reader;

import org.apache.iotdb.db.storageengine.dataregion.compaction.tool.Interval;
import org.apache.iotdb.db.storageengine.dataregion.compaction.tool.TsFileStatisticReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.tool.UnseqSpaceStatistics;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

public class SingleSequenceFileTask implements Callable<TaskSummary> {
  private UnseqSpaceStatistics unseqSpaceStatistics;
  private String seqFile;

  public SingleSequenceFileTask(UnseqSpaceStatistics unseqSpaceStatistics, String seqFile) {
    this.unseqSpaceStatistics = unseqSpaceStatistics;
    this.seqFile = seqFile;
  }

  @Override
  public TaskSummary call() throws Exception {
    return checkSeqFile(unseqSpaceStatistics, seqFile);
  }

  private TaskSummary checkSeqFile(UnseqSpaceStatistics unseqSpaceStatistics, String seqFile) {
    TaskSummary summary = new TaskSummary();
    try (TsFileStatisticReader reader = new TsFileStatisticReader(seqFile)) {
      String currentDevice = reader.currentDevice();
      long deviceStartTime = Long.MAX_VALUE, deviceEndTime = Long.MIN_VALUE;
      // empty file
      if (currentDevice == null) {
        return new TaskSummary();
      }
      long chunkGroupNum = 1;
      // check chunk overlap
      Set<String> set = new HashSet<>();
      while (reader.hasNextSeries()) {
        String nextDevice = reader.currentDevice();
        List<ChunkMetadata> chunkMetadataList = reader.nextSeries();
        // previous chunk group finish
        if (!currentDevice.equals(nextDevice)) {
          if (deviceStartTime <= deviceEndTime) {
            if (unseqSpaceStatistics.chunkGroupHasOverlap(
                currentDevice, new Interval(deviceStartTime, deviceEndTime))) {
              summary.overlapChunkGroup++;
            }
          }
          // new device
          currentDevice = nextDevice;
          deviceStartTime = Long.MAX_VALUE;
          deviceEndTime = Long.MIN_VALUE;
          chunkGroupNum++;
        }
        for (ChunkMetadata chunkMetadata : chunkMetadataList) {
          long chunkStartTime = chunkMetadata.getStartTime();
          long chunkEndTime = chunkMetadata.getEndTime();
          // update device time
          deviceStartTime = Math.min(deviceStartTime, chunkStartTime);
          deviceEndTime = Math.max(deviceEndTime, chunkEndTime);
          // skip empty chunk
          if (chunkStartTime > chunkEndTime) {
            System.out.println(chunkMetadata);
            continue;
          }
          Interval interval = new Interval(chunkStartTime, chunkEndTime);
          if (unseqSpaceStatistics.chunkHasOverlap(
              currentDevice, chunkMetadata.getMeasurementUid(), interval)) {
            summary.overlapChunk++;
          }
        }
        summary.totalChunks += chunkMetadataList.size();
        chunkMetadataList.clear();
      }
      if (unseqSpaceStatistics.chunkGroupHasOverlap(
          reader.currentDevice(), new Interval(deviceStartTime, deviceEndTime))) {
        summary.overlapChunkGroup++;
      }
      summary.totalChunkGroups = chunkGroupNum;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return summary;
  }
}
