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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.ModifiedStatus;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileReader;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.TimeRange;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class LazyChunkLoader implements ChunkLoader {
  private CompactionTsFileReader reader;
  private ChunkMetadata chunkMetadata;
  private Chunk chunk;
  private ChunkHeader chunkHeader;
  private ModifiedStatus modifiedStatus;

  public LazyChunkLoader(CompactionTsFileReader reader, ChunkMetadata chunkMetadata) {
    this.reader = reader;
    this.chunkMetadata = chunkMetadata;
    calculateModifiedStatus();
  }

  private void calculateModifiedStatus() {
    if (modifiedStatus != null) {
      return;
    }
    this.modifiedStatus = ModifiedStatus.NONE_DELETED;
    List<TimeRange> deleteIntervalList = chunkMetadata.getDeleteIntervalList();
    if (deleteIntervalList == null || deleteIntervalList.isEmpty()) {
      return;
    }
    long startTime = chunkMetadata.getStartTime();
    long endTime = chunkMetadata.getEndTime();
    TimeRange chunkTimeRange = new TimeRange(startTime, endTime);
    for (TimeRange timeRange : deleteIntervalList) {
      if (timeRange.contains(chunkTimeRange)) {
        this.modifiedStatus = ModifiedStatus.ALL_DELETED;
        break;
      } else if (timeRange.overlaps(chunkTimeRange)) {
        this.modifiedStatus = ModifiedStatus.PARTIAL_DELETED;
      }
    }
  }

  public LazyChunkLoader() {}

  @Override
  public Chunk getChunk() throws IOException {
    if (reader == null) {
      return null;
    }
    if (chunkHeader == null) {
      this.chunk = reader.readMemChunk(chunkMetadata);
      this.chunkHeader = this.chunk.getHeader();
      return this.chunk;
    }
    ByteBuffer buffer =
        reader.readChunk(
            chunkMetadata.getOffsetOfChunkHeader() + chunkHeader.getSerializedSize(),
            chunkHeader.getDataSize());
    this.chunk =
        new Chunk(
            chunkHeader,
            buffer,
            chunkMetadata.getDeleteIntervalList(),
            chunkMetadata.getStatistics());
    return this.chunk;
  }

  @Override
  public ChunkMetadata getChunkMetadata() {
    return chunkMetadata;
  }

  @Override
  public boolean isEmpty() {
    return reader == null;
  }

  @Override
  public ChunkHeader getHeader() throws IOException {
    if (isEmpty()) {
      return null;
    }
    if (chunkHeader != null) {
      return chunkHeader;
    }
    chunkHeader = reader.readChunkHeader(chunkMetadata.getOffsetOfChunkHeader());
    return chunkHeader;
  }

  @Override
  public ModifiedStatus getModifiedStatus() {
    return this.modifiedStatus;
  }

  @Override
  public List<PageLoader> getPages() throws IOException {
    long chunkDataStartOffset =
        chunkMetadata.getOffsetOfChunkHeader() + chunkHeader.getSerializedSize();
    long chunkEndOffset = chunkDataStartOffset + chunkHeader.getDataSize();
    long index = chunkDataStartOffset;
    boolean hasPageStatistics =
        ((byte) (chunkHeader.getChunkType() & 0x3F)) != MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER;
    List<PageLoader> pageLoaders = new ArrayList<>();
    reader.position(index);
    InputStream inputStream = reader.wrapAsInputStream();
    while (index < chunkEndOffset) {
      PageHeader pageHeader =
          PageHeader.deserializeFrom(inputStream, chunkHeader.getDataType(), hasPageStatistics);
      int serializedPageSize = pageHeader.getSerializedPageSize();
      int headerSize = serializedPageSize - pageHeader.getCompressedSize();
      if (!hasPageStatistics) {
        pageHeader.setStatistics(chunkMetadata.getStatistics());
      }
      ModifiedStatus pageModifiedStatus = calculatePageModifiedStatus(pageHeader);
      pageLoaders.add(
          new LazyPageLoader(
              reader,
              pageHeader,
              index + headerSize,
              chunk.getHeader().getCompressionType(),
              chunk.getHeader().getDataType(),
              chunk.getHeader().getEncodingType(),
              chunkMetadata.getDeleteIntervalList(),
              pageModifiedStatus));
      index += serializedPageSize;
      inputStream.skip(pageHeader.getCompressedSize());
    }
    return pageLoaders;
  }

  private ModifiedStatus calculatePageModifiedStatus(PageHeader pageHeader) {
    if (this.modifiedStatus != ModifiedStatus.PARTIAL_DELETED) {
      return this.modifiedStatus;
    }
    ModifiedStatus pageModifiedStatus = ModifiedStatus.NONE_DELETED;
    List<TimeRange> deleteIntervalList = chunkMetadata.getDeleteIntervalList();
    long startTime = pageHeader.getStartTime();
    long endTime = pageHeader.getEndTime();
    TimeRange pageTimeRange = new TimeRange(startTime, endTime);
    for (TimeRange timeRange : deleteIntervalList) {
      if (timeRange.contains(pageTimeRange)) {
        pageModifiedStatus = ModifiedStatus.ALL_DELETED;
        break;
      } else if (timeRange.overlaps(pageTimeRange)) {
        pageModifiedStatus = ModifiedStatus.PARTIAL_DELETED;
      }
    }
    return pageModifiedStatus;
  }

  @Override
  public void clear() {
    this.chunkHeader = null;
    this.chunkMetadata = null;
    this.chunk = null;
  }
}
