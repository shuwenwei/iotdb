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

package org.apache.iotdb.db.storageengine.dataregion.compaction.tool;

import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TsFileStatisticReader implements Closeable {

  private final TsFileSequenceReader reader;

  public TsFileStatisticReader(String filePath) throws IOException {
    reader = new TsFileSequenceReader(filePath);
  }

  public List<ChunkGroupStatistics> getChunkGroupStatistics() throws IOException {
    Map<String, List<TimeseriesMetadata>> allTimeseriesMetadata = reader.getAllTimeseriesMetadata(true);
    List<ChunkGroupStatistics> chunkGroupStatisticsList = new ArrayList<>();
    for (Map.Entry<String, List<TimeseriesMetadata>> deviceTimeSeriesMetadataListEntry : allTimeseriesMetadata.entrySet()) {
      String deviceId = deviceTimeSeriesMetadataListEntry.getKey();
      List<TimeseriesMetadata> deviceMetadataList = deviceTimeSeriesMetadataListEntry.getValue();
      if (deviceMetadataList.isEmpty()) {
        continue;
      }
      ChunkGroupStatistics chunkGroupStatistics = new ChunkGroupStatistics(deviceId, deviceMetadataList.get(0).getMeasurementId().isEmpty());
      for (TimeseriesMetadata timeseriesMetadata : deviceMetadataList) {
        chunkGroupStatistics.chunkMetadataList.addAll(timeseriesMetadata.getChunkMetadataList());
      }
    }
    return chunkGroupStatisticsList;
  }

  @Override
  public void close() throws IOException {
    this.reader.close();
  }

  public static class ChunkGroupStatistics {
    private final String deviceID;
    private final List<IChunkMetadata> chunkMetadataList;
    private final boolean isAligned;

    private ChunkGroupStatistics(String deviceId, boolean isAligned) {
      this.deviceID = deviceId;
      this.isAligned = isAligned;
      this.chunkMetadataList = new ArrayList<>();
    }

    public String getDeviceID() {
      return deviceID;
    }

    public List<IChunkMetadata> getChunkMetadataList() {
      return chunkMetadataList;
    }

    public int getTotalChunkNum() {
      return chunkMetadataList.size();
    }

    public boolean isAligned() {
      return isAligned;
    }
  }
}
