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
import org.apache.iotdb.db.storageengine.dataregion.compaction.tool.TimePartitionProcessTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.tool.UnseqSpaceStatistics;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileDeviceIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SingleUnSequenceFileTask implements Callable<TaskSummary> {

  private UnseqSpaceStatistics statistics;
  private ConcurrentLinkedQueue<TimePartitionProcessTask.UpdateInterval> processQueue;
  private String filePath;

  public SingleUnSequenceFileTask(
      UnseqSpaceStatistics statistics,
      ConcurrentLinkedQueue<TimePartitionProcessTask.UpdateInterval> processQueue,
      String filePath) {
    this.statistics = statistics;
    this.filePath = filePath;
    this.processQueue = processQueue;
  }

  @Override
  public TaskSummary call() throws Exception {
    processUnSequenceFile();
    return null;
  }

  private void processUnSequenceFile() {
    try (TsFileStatisticReaderV2 reader = new TsFileStatisticReaderV2(filePath)) {
      TsFileDeviceIterator deviceIterator = reader.getDeviceIterator();
      while (deviceIterator.hasNext()) {
        String device = deviceIterator.next().left;
        long deviceStartTime = Long.MAX_VALUE, deviceEndTime = Long.MIN_VALUE;
        Iterator<List<ChunkMetadata>> chunkMetadataListIterator =
            reader.getDeviceChunkMetadataListIterator(device);
        List<ChunkMetadata> chunkMetadataList;
        while (chunkMetadataListIterator.hasNext()) {
          chunkMetadataList = chunkMetadataListIterator.next();

          for (int i = 0; i < chunkMetadataList.size(); i++) {
            ChunkMetadata chunkMetadata = chunkMetadataList.get(i);

            long chunkStartTime = chunkMetadata.getStartTime();
            long chunkEndTime = chunkMetadata.getEndTime();
            deviceStartTime = Math.min(deviceStartTime, chunkStartTime);
            deviceEndTime = Math.max(deviceEndTime, chunkEndTime);
            if (chunkStartTime <= chunkEndTime) {
              processQueue.add(
                  new TimePartitionProcessTask.UpdateInterval(
                      device,
                      chunkMetadata.getMeasurementUid(),
                      new Interval(chunkStartTime, chunkEndTime)));
            }
          }
        }
        if (deviceStartTime <= deviceEndTime) {
          statistics.lock.lock();
          statistics.updateDevice(device, new Interval(deviceStartTime, deviceEndTime));
          statistics.lock.unlock();
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
