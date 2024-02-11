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

package org.apache.iotdb.db.storageengine.dataregion.compaction.repair;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionLastTimeCheckFailedException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.reader.CompactionChunkReader;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileDeviceIterator;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RepairDataFileScanUtil {
  private final TsFileResource resource;
  private boolean hasUnsortedData;
  private boolean isBrokenFile;
  private long previousTime;

  public RepairDataFileScanUtil(TsFileResource resource) {
    this.resource = resource;
    this.hasUnsortedData = false;
    this.previousTime = Long.MIN_VALUE;
  }

  public void scanTsFile() {
    File tsfile = resource.getTsFile();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsfile.getPath())) {
      TsFileDeviceIterator deviceIterator = reader.getAllDevicesIteratorWithIsAligned();
      while (deviceIterator.hasNext()) {
        Pair<String, Boolean> deviceIsAlignedPair = deviceIterator.next();
        String device = deviceIsAlignedPair.getLeft();
        boolean isAligned = deviceIsAlignedPair.getRight();
        if (isAligned) {
          checkAlignedDeviceSeries(reader, device);
        } else {
          checkNonAlignedDeviceSeries(reader, device);
        }
      }
    } catch (IOException ignored) {
      isBrokenFile = true;
    } catch (CompactionLastTimeCheckFailedException lastTimeCheckFailedException) {
      this.hasUnsortedData = true;
    }
  }

  private void checkAlignedDeviceSeries(TsFileSequenceReader reader, String device)
      throws IOException {
    List<AlignedChunkMetadata> chunkMetadataList = reader.getAlignedChunkMetadata(device);
    for (AlignedChunkMetadata alignedChunkMetadata : chunkMetadataList) {
      IChunkMetadata timeChunkMetadata = alignedChunkMetadata.getTimeChunkMetadata();
      Chunk timeChunk = reader.readMemChunk((ChunkMetadata) timeChunkMetadata);

      CompactionChunkReader chunkReader = new CompactionChunkReader(timeChunk);
      ByteBuffer chunkDataBuffer = timeChunk.getData();
      ChunkHeader chunkHeader = timeChunk.getHeader();
      while (chunkDataBuffer.hasRemaining()) {
        // deserialize a PageHeader from chunkDataBuffer
        PageHeader pageHeader = null;
        if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
          pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, timeChunk.getChunkStatistic());
        } else {
          pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
        }
        ByteBuffer pageData = chunkReader.readPageDataWithoutUncompressing(pageHeader);

        ByteBuffer uncompressedPageData =
            uncompressPageData(chunkHeader.getCompressionType(), pageHeader, pageData);
        Decoder decoder =
            Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());
        while (decoder.hasNext(uncompressedPageData)) {
          long currentTime = decoder.readLong(uncompressedPageData);
          checkPreviousTimeAndUpdate(device, currentTime);
        }
      }
    }
    previousTime = Long.MIN_VALUE;
  }

  private void checkNonAlignedDeviceSeries(TsFileSequenceReader reader, String device)
      throws IOException {
    Iterator<Map<String, List<ChunkMetadata>>> measurementChunkMetadataListMapIterator =
        reader.getMeasurementChunkMetadataListMapIterator(device);
    while (measurementChunkMetadataListMapIterator.hasNext()) {
      Map<String, List<ChunkMetadata>> measurementChunkMetadataListMap =
          measurementChunkMetadataListMapIterator.next();
      for (Map.Entry<String, List<ChunkMetadata>> measurementChunkMetadataListEntry :
          measurementChunkMetadataListMap.entrySet()) {
        String measurement = measurementChunkMetadataListEntry.getKey();
        List<ChunkMetadata> chunkMetadataList = measurementChunkMetadataListEntry.getValue();
        checkSingleNonAlignedSeries(reader, measurement, chunkMetadataList);
        previousTime = Long.MIN_VALUE;
      }
    }
  }

  private void checkSingleNonAlignedSeries(
      TsFileSequenceReader reader, String measurement, List<ChunkMetadata> chunkMetadataList)
      throws IOException {
    for (ChunkMetadata chunkMetadata : chunkMetadataList) {
      if (chunkMetadata == null || chunkMetadata.getStatistics().getCount() == 0) {
        continue;
      }
      Chunk chunk = reader.readMemChunk(chunkMetadata);
      ChunkReader chunkReader = new ChunkReader(chunk);
      while (chunkReader.hasNextSatisfiedPage()) {
        IPointReader pointReader = chunkReader.nextPageData().getBatchDataIterator();
        while (pointReader.hasNextTimeValuePair()) {
          TimeValuePair timeValuePair = pointReader.nextTimeValuePair();
          checkPreviousTimeAndUpdate(measurement, timeValuePair.getTimestamp());
        }
      }
    }
  }

  private ByteBuffer uncompressPageData(
      CompressionType compressionType, PageHeader pageHeader, ByteBuffer pageData)
      throws IOException {
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(compressionType);
    byte[] uncompressedData = new byte[pageHeader.getUncompressedSize()];
    unCompressor.uncompress(
        pageData.array(),
        0,
        pageHeader.getCompressedSize(),
        uncompressedData,
        pageHeader.getUncompressedSize());
    return ByteBuffer.wrap(uncompressedData);
  }

  private void checkPreviousTimeAndUpdate(String path, long time) {
    if (previousTime >= time) {
      throw new CompactionLastTimeCheckFailedException(path, time, previousTime);
    }
    previousTime = time;
  }

  public boolean hasUnsortedData() {
    return hasUnsortedData;
  }

  public boolean isBrokenFile() {
    return isBrokenFile;
  }

  public static List<TsFileResource> checkTimePartitionHasOverlap(List<TsFileResource> resources) {
    List<TsFileResource> overlapResources = new ArrayList<>();
    Map<String, Long> deviceEndTimeMap = new HashMap<>();
    for (TsFileResource resource : resources) {
      if (resource.getStatus() == TsFileResourceStatus.UNCLOSED
          || resource.getStatus() == TsFileResourceStatus.DELETED) {
        continue;
      }
      DeviceTimeIndex deviceTimeIndex;
      try {
        deviceTimeIndex = getDeviceTimeIndex(resource);
      } catch (Exception ignored) {
        continue;
      }

      Set<String> devices = deviceTimeIndex.getDevices();
      boolean fileHasOverlap = false;
      // check overlap
      for (String device : devices) {
        long deviceStartTimeInCurrentFile = deviceTimeIndex.getStartTime(device);
        if (deviceStartTimeInCurrentFile > deviceTimeIndex.getEndTime(device)) {
          continue;
        }
        if (!deviceEndTimeMap.containsKey(device)) {
          continue;
        }
        long deviceEndTimeInPreviousFile = deviceEndTimeMap.get(device);
        if (deviceStartTimeInCurrentFile <= deviceEndTimeInPreviousFile) {
          fileHasOverlap = true;
          overlapResources.add(resource);
          break;
        }
      }
      // update end time map
      if (!fileHasOverlap) {
        for (String device : devices) {
          deviceEndTimeMap.put(device, deviceTimeIndex.getEndTime(device));
        }
      }
    }
    return overlapResources;
  }

  private static DeviceTimeIndex getDeviceTimeIndex(TsFileResource resource) throws IOException {
    ITimeIndex timeIndex = resource.getTimeIndex();
    if (timeIndex instanceof DeviceTimeIndex) {
      return (DeviceTimeIndex) timeIndex;
    }
    return resource.buildDeviceTimeIndex();
  }
}
