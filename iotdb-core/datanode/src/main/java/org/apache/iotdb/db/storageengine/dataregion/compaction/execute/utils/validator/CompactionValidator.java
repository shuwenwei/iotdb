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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.validator;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionValidationLevel;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.read.reader.page.TimePageReader;
import org.apache.iotdb.tsfile.read.reader.page.ValuePageReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface CompactionValidator {

   Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  boolean validateCompaction(String storageGroupName, TsFileManager manager, long timePartition, List<TsFileResource> sourceSeqTsFileList, List<TsFileResource> sourceUnSeqFileList, List<TsFileResource> targetTsFileList, boolean isInnerUnSequenceSpaceTask, boolean isInPlaceCrossSpaceCompaction) throws IOException;

  static CompactionValidator getInstance() {
    CompactionValidationLevel level =
        IoTDBDescriptor.getInstance().getConfig().getCompactionValidationLevel();
    switch (level) {
      case NONE:
        return NoneCompactionValidator.getInstance();
      case RESOURCE_ONLY:
        return ResourceOnlyCompactionValidator.getInstance();
      default:
        return ResourceAndTsfileCompactionValidator.getInstance();
    }
  }

  default boolean validateTsFileResources(String storageGroupName, TsFileManager tsFileManager, long timePartition, List<TsFileResource> sourceSeqFiles, List<TsFileResource> targetFiles) throws IOException {
    List<TsFileResource> timePartitionSeqFiles = tsFileManager.getCopyOfSequenceListByTimePartition(timePartition);
    timePartitionSeqFiles.removeAll(sourceSeqFiles);
    timePartitionSeqFiles.addAll(targetFiles);

    timePartitionSeqFiles.sort(
        (f1, f2) -> {
          int timeDiff =
              Long.compareUnsigned(
                  Long.parseLong(f1.getTsFile().getName().split("-")[0]),
                  Long.parseLong(f2.getTsFile().getName().split("-")[0]));
          return timeDiff == 0
              ? Long.compareUnsigned(
              Long.parseLong(f1.getTsFile().getName().split("-")[1]),
              Long.parseLong(f2.getTsFile().getName().split("-")[1]))
              : timeDiff;
        });
    // deviceID -> <TsFileResource, last end time>
    Map<String, Pair<TsFileResource, Long>> lastEndTimeMap = new HashMap<>();
    for (TsFileResource resource : timePartitionSeqFiles) {
      DeviceTimeIndex timeIndex;
      if (resource.getTimeIndexType() != 1) {
        // if time index is not device time index, then deserialize it from resource file
        timeIndex = resource.buildDeviceTimeIndex();
      } else {
        timeIndex = (DeviceTimeIndex) resource.getTimeIndex();
      }
      Set<String> devices = timeIndex.getDevices();
      for (String device : devices) {
        long currentStartTime = timeIndex.getStartTime(device);
        long currentEndTime = timeIndex.getEndTime(device);
        Pair<TsFileResource, Long> lastDeviceInfo =
            lastEndTimeMap.computeIfAbsent(device, x -> new Pair<>(null, Long.MIN_VALUE));
        long lastEndTime = lastDeviceInfo.right;
        if (lastEndTime >= currentStartTime) {
          logger.error(
              "{} Device {} is overlapped between {} and {}, "
                  + "end time in {} is {}, start time in {} is {}",
              storageGroupName,
              device,
              lastDeviceInfo.left,
              resource,
              lastDeviceInfo.left,
              lastEndTime,
              resource,
              currentStartTime);
          return false;
        }
        lastDeviceInfo.left = resource;
        lastDeviceInfo.right = currentEndTime;
        lastEndTimeMap.put(device, lastDeviceInfo);
      }
    }
    return true;
  }

  /**
   * Validate TsFiles by reading them sequentially. This method should be fast because the read is
   * sequential.
   *
   * @param tsFilesToValidate the tsfiles to be checked
   * @return true if all tsfiles are valid, false if any of the tsfiles is invalid
   */
  default boolean validateTsFiles(List<TsFileResource> tsFilesToValidate) {
    for (TsFileResource tsFileResource : tsFilesToValidate) {
      if (!validateSingleTsFile(tsFileResource)) {
        return false;
      }
    }
    return true;
  }

  default boolean validateSingleTsFile(TsFileResource resource) {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(resource.getTsFilePath())) {
      reader.readHeadMagic();
      reader.readTailMagic();

      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      List<long[]> timeBatch = new ArrayList<>();
      int pageIndex = 0;
      byte marker;
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.TIME_CHUNK_HEADER:
          case MetaMarker.VALUE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
            ChunkHeader header = reader.readChunkHeader(marker);
            if (header.getDataSize() == 0) {
              // empty value chunk
              break;
            }
            Decoder defaultTimeDecoder =
                Decoder.getDecoderByType(
                    TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                    TSDataType.INT64);
            Decoder valueDecoder =
                Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
            int dataSize = header.getDataSize();
            pageIndex = 0;
            if (header.getDataType() == TSDataType.VECTOR) {
              timeBatch.clear();
            }
            while (dataSize > 0) {
              valueDecoder.reset();
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(),
                      (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
              if ((header.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
                  == TsFileConstant.TIME_COLUMN_MASK) { // Time Chunk
                TimePageReader timePageReader =
                    new TimePageReader(pageHeader, pageData, defaultTimeDecoder);
                timeBatch.add(timePageReader.getNextTimeBatch());
              } else if ((header.getChunkType() & TsFileConstant.VALUE_COLUMN_MASK)
                  == TsFileConstant.VALUE_COLUMN_MASK) { // Value Chunk
                ValuePageReader valuePageReader =
                    new ValuePageReader(pageHeader, pageData, header.getDataType(), valueDecoder);
                TsPrimitiveType[] valueBatch =
                    valuePageReader.nextValueBatch(timeBatch.get(pageIndex));
              } else { // NonAligned Chunk
                PageReader pageReader =
                    new PageReader(
                        pageData, header.getDataType(), valueDecoder, defaultTimeDecoder, null);
                BatchData batchData = pageReader.getAllSatisfiedPageData();
              }
              pageIndex++;
              dataSize -= pageHeader.getSerializedPageSize();
            }
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }
      for (String device : reader.getAllDevices()) {
        Map<String, List<ChunkMetadata>> seriesMetaData = reader.readChunkMetadataInDevice(device);
      }
    } catch (Exception e) {
      logger.error("Meets error when validating TsFile {}, ", resource.getTsFilePath(), e);
      return false;
    }
    return true;
  }
}
