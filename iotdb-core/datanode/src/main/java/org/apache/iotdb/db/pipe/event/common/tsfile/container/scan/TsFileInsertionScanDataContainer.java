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

package org.apache.iotdb.db.pipe.event.common.tsfile.container.scan;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.container.TsFileInsertionDataContainer;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.IChunkReader;
import org.apache.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

public class TsFileInsertionScanDataContainer extends TsFileInsertionDataContainer {

  private final long startTime;
  private final long endTime;
  private final Filter filter;

  private IChunkReader chunkReader;
  private BatchData data;

  private boolean isMultiPage;
  private IDeviceID currentDevice;
  private boolean currentIsAligned;
  private final List<IMeasurementSchema> currentMeasurements = new ArrayList<>();

  // Cached time chunk
  private final List<Chunk> timeChunkList = new ArrayList<>();
  private final Map<String, Integer> measurementIndexMap = new HashMap<>();
  private int lastIndex = -1;
  private ChunkHeader firstChunkHeader4NextSequentialValueChunks;

  private byte lastMarker = Byte.MIN_VALUE;

  public TsFileInsertionScanDataContainer(
      final File tsFile,
      final PipePattern pattern,
      final long startTime,
      final long endTime,
      final PipeTaskMeta pipeTaskMeta,
      final EnrichedEvent sourceEvent)
      throws IOException {
    super(pattern, startTime, endTime, pipeTaskMeta, sourceEvent);

    this.startTime = startTime;
    this.endTime = endTime;
    filter = Objects.nonNull(timeFilterExpression) ? timeFilterExpression.getFilter() : null;

    try {
      tsFileSequenceReader = new TsFileSequenceReader(tsFile.getAbsolutePath(), false, false);
      tsFileSequenceReader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);

      prepareData();
    } catch (final Exception e) {
      close();
      throw e;
    }
  }

  @Override
  public Iterable<TabletInsertionEvent> toTabletInsertionEvents() {
    return () ->
        new Iterator<TabletInsertionEvent>() {

          @Override
          public boolean hasNext() {
            return Objects.nonNull(chunkReader);
          }

          @Override
          public TabletInsertionEvent next() {
            if (!hasNext()) {
              close();
              throw new NoSuchElementException();
            }

            final Tablet tablet = getNextTablet();
            final boolean hasNext = hasNext();
            try {
              return new PipeRawTabletInsertionEvent(
                  tablet,
                  currentIsAligned,
                  sourceEvent != null ? sourceEvent.getPipeName() : null,
                  sourceEvent != null ? sourceEvent.getCreationTime() : 0,
                  pipeTaskMeta,
                  sourceEvent,
                  !hasNext);
            } finally {
              if (!hasNext) {
                close();
              }
            }
          }
        };
  }

  public Iterable<Tablet> toTablets() {
    return () ->
        new Iterator<Tablet>() {
          @Override
          public boolean hasNext() {
            return Objects.nonNull(chunkReader);
          }

          @Override
          public Tablet next() {
            if (!hasNext()) {
              close();
              throw new NoSuchElementException();
            }

            final Tablet tablet = getNextTablet();
            final boolean hasNext = hasNext();
            try {
              return tablet;
            } finally {
              if (!hasNext) {
                close();
              }
            }
          }
        };
  }

  private Tablet getNextTablet() {
    try {
      Tablet tablet = null;

      if (!data.hasCurrent()) {
        tablet = new Tablet(currentDevice.toString(), currentMeasurements, 1);
        tablet.initBitMaps();
        // Ignore the memory cost of tablet
        PipeDataNodeResourceManager.memory().forceResize(allocatedMemoryBlockForTablet, 0);
        return tablet;
      }

      boolean isFirstRow = true;
      while (data.hasCurrent()) {
        if (isMultiPage || data.currentTime() >= startTime && data.currentTime() <= endTime) {
          if (isFirstRow) {
            // Calculate row count and memory size of the tablet based on the first row
            Pair<Integer, Integer> rowCountAndMemorySize =
                PipeMemoryWeightUtil.calculateTabletRowCountAndMemory(data);
            tablet =
                new Tablet(
                    currentDevice.toString(), currentMeasurements, rowCountAndMemorySize.getLeft());
            tablet.initBitMaps();
            PipeDataNodeResourceManager.memory()
                .forceResize(allocatedMemoryBlockForTablet, rowCountAndMemorySize.getRight());
            isFirstRow = false;
          }

          final int rowIndex = tablet.rowSize;

          tablet.addTimestamp(rowIndex, data.currentTime());
          putValueToColumns(data, tablet.values, tablet.bitMaps, rowIndex);

          tablet.rowSize++;
        }

        data.next();
        while (!data.hasCurrent() && chunkReader.hasNextSatisfiedPage()) {
          data = chunkReader.nextPageData();
        }

        if (tablet != null && tablet.rowSize == tablet.getMaxRowNumber()) {
          break;
        }
      }

      if (tablet == null) {
        tablet = new Tablet(currentDevice.toString(), currentMeasurements, 1);
        tablet.initBitMaps();
        // Ignore the memory cost of tablet
        PipeDataNodeResourceManager.memory().forceResize(allocatedMemoryBlockForTablet, 0);
      }

      // Switch chunk reader iff current chunk is all consumed
      if (!data.hasCurrent()) {
        prepareData();
      }
      return tablet;
    } catch (final Exception e) {
      close();
      throw new PipeException("Failed to get next tablet insertion event.", e);
    }
  }

  private void prepareData() throws IOException {
    do {
      do {
        moveToNextChunkReader();
      } while (Objects.nonNull(chunkReader) && !chunkReader.hasNextSatisfiedPage());

      if (Objects.isNull(chunkReader)) {
        close();
        break;
      }

      do {
        data = chunkReader.nextPageData();
      } while (!data.hasCurrent() && chunkReader.hasNextSatisfiedPage());
    } while (!data.hasCurrent());
  }

  private void putValueToColumns(
      final BatchData data, final Object[] columns, final BitMap[] bitMaps, final int rowIndex) {
    final TSDataType type = data.getDataType();
    if (type == TSDataType.VECTOR) {
      for (int i = 0; i < columns.length; ++i) {
        final TsPrimitiveType primitiveType = data.getVector()[i];
        if (Objects.isNull(primitiveType)) {
          bitMaps[i].mark(rowIndex);
          continue;
        }
        switch (primitiveType.getDataType()) {
          case BOOLEAN:
            ((boolean[]) columns[i])[rowIndex] = primitiveType.getBoolean();
            break;
          case INT32:
            ((int[]) columns[i])[rowIndex] = primitiveType.getInt();
            break;
          case DATE:
            ((LocalDate[]) columns[i])[rowIndex] =
                DateUtils.parseIntToLocalDate(primitiveType.getInt());
            break;
          case INT64:
          case TIMESTAMP:
            ((long[]) columns[i])[rowIndex] = primitiveType.getLong();
            break;
          case FLOAT:
            ((float[]) columns[i])[rowIndex] = primitiveType.getFloat();
            break;
          case DOUBLE:
            ((double[]) columns[i])[rowIndex] = primitiveType.getDouble();
            break;
          case TEXT:
          case BLOB:
          case STRING:
            ((Binary[]) columns[i])[rowIndex] = primitiveType.getBinary();
            break;
          default:
            throw new UnSupportedDataTypeException("UnSupported" + primitiveType.getDataType());
        }
      }
    } else {
      switch (type) {
        case BOOLEAN:
          ((boolean[]) columns[0])[rowIndex] = data.getBoolean();
          break;
        case INT32:
          ((int[]) columns[0])[rowIndex] = data.getInt();
          break;
        case DATE:
          ((LocalDate[]) columns[0])[rowIndex] = DateUtils.parseIntToLocalDate(data.getInt());
          break;
        case INT64:
        case TIMESTAMP:
          ((long[]) columns[0])[rowIndex] = data.getLong();
          break;
        case FLOAT:
          ((float[]) columns[0])[rowIndex] = data.getFloat();
          break;
        case DOUBLE:
          ((double[]) columns[0])[rowIndex] = data.getDouble();
          break;
        case TEXT:
        case BLOB:
        case STRING:
          ((Binary[]) columns[0])[rowIndex] = data.getBinary();
          break;
        default:
          throw new UnSupportedDataTypeException("UnSupported" + data.getDataType());
      }
    }
  }

  private void moveToNextChunkReader() throws IOException, IllegalStateException {
    ChunkHeader chunkHeader;
    final List<Chunk> valueChunkList = new ArrayList<>();
    currentMeasurements.clear();

    if (lastMarker == MetaMarker.SEPARATOR) {
      chunkReader = null;
      return;
    }

    byte marker;
    while ((marker = lastMarker != Byte.MIN_VALUE ? lastMarker : tsFileSequenceReader.readMarker())
        != MetaMarker.SEPARATOR) {
      lastMarker = Byte.MIN_VALUE;
      switch (marker) {
        case MetaMarker.CHUNK_HEADER:
        case MetaMarker.TIME_CHUNK_HEADER:
        case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
        case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
          // Notice that the data in one chunk group is either aligned or non-aligned
          // There is no need to consider non-aligned chunks when there are value chunks
          isMultiPage = marker == MetaMarker.CHUNK_HEADER || marker == MetaMarker.TIME_CHUNK_HEADER;

          chunkHeader = tsFileSequenceReader.readChunkHeader(marker);

          if (Objects.isNull(currentDevice)) {
            tsFileSequenceReader.position(
                tsFileSequenceReader.position() + chunkHeader.getDataSize());
            break;
          }

          if ((chunkHeader.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
              == TsFileConstant.TIME_COLUMN_MASK) {
            timeChunkList.add(
                new Chunk(
                    chunkHeader, tsFileSequenceReader.readChunk(-1, chunkHeader.getDataSize())));
            break;
          }

          if (!pattern.matchesMeasurement(currentDevice, chunkHeader.getMeasurementID())) {
            tsFileSequenceReader.position(
                tsFileSequenceReader.position() + chunkHeader.getDataSize());
            break;
          }

          chunkReader =
              isMultiPage
                  ? new ChunkReader(
                      new Chunk(
                          chunkHeader,
                          tsFileSequenceReader.readChunk(-1, chunkHeader.getDataSize())),
                      filter)
                  : new SinglePageWholeChunkReader(
                      new Chunk(
                          chunkHeader,
                          tsFileSequenceReader.readChunk(-1, chunkHeader.getDataSize())));
          currentIsAligned = false;
          currentMeasurements.add(
              new MeasurementSchema(chunkHeader.getMeasurementID(), chunkHeader.getDataType()));
          return;
        case MetaMarker.VALUE_CHUNK_HEADER:
        case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
          if (Objects.isNull(firstChunkHeader4NextSequentialValueChunks)) {
            chunkHeader = tsFileSequenceReader.readChunkHeader(marker);

            if (Objects.isNull(currentDevice)
                || !pattern.matchesMeasurement(currentDevice, chunkHeader.getMeasurementID())) {
              tsFileSequenceReader.position(
                  tsFileSequenceReader.position() + chunkHeader.getDataSize());
              break;
            }

            // Increase value index
            final int valueIndex =
                measurementIndexMap.compute(
                    chunkHeader.getMeasurementID(),
                    (measurement, index) -> Objects.nonNull(index) ? index + 1 : 0);

            // Emit when encountered non-sequential value chunk
            // Do not record or end current value chunks when there are empty chunks
            if (chunkHeader.getDataSize() == 0) {
              break;
            }
            boolean needReturn = false;
            if (lastIndex >= 0 && valueIndex != lastIndex) {
              needReturn = recordAlignedChunk(valueChunkList, marker);
            }
            lastIndex = valueIndex;
            if (needReturn) {
              firstChunkHeader4NextSequentialValueChunks = chunkHeader;
              return;
            }
          } else {
            chunkHeader = firstChunkHeader4NextSequentialValueChunks;
            firstChunkHeader4NextSequentialValueChunks = null;
          }

          valueChunkList.add(
              new Chunk(
                  chunkHeader, tsFileSequenceReader.readChunk(-1, chunkHeader.getDataSize())));
          currentMeasurements.add(
              new MeasurementSchema(chunkHeader.getMeasurementID(), chunkHeader.getDataType()));
          break;
        case MetaMarker.CHUNK_GROUP_HEADER:
          // Return before "currentDevice" changes
          if (recordAlignedChunk(valueChunkList, marker)) {
            return;
          }
          // Clear because the cached data will never be used in the next chunk group
          lastIndex = -1;
          timeChunkList.clear();
          measurementIndexMap.clear();
          final IDeviceID deviceID = tsFileSequenceReader.readChunkGroupHeader().getDeviceID();
          currentDevice = pattern.mayOverlapWithDevice(deviceID) ? deviceID : null;
          break;
        case MetaMarker.OPERATION_INDEX_RANGE:
          tsFileSequenceReader.readPlanIndex();
          break;
        default:
          MetaMarker.handleUnexpectedMarker(marker);
      }
    }

    lastMarker = marker;
    if (!recordAlignedChunk(valueChunkList, marker)) {
      chunkReader = null;
    }
  }

  private boolean recordAlignedChunk(final List<Chunk> valueChunkList, final byte marker)
      throws IOException {
    if (!valueChunkList.isEmpty()) {
      chunkReader =
          isMultiPage
              ? new AlignedChunkReader(timeChunkList.get(lastIndex), valueChunkList, filter)
              : new AlignedSinglePageWholeChunkReader(timeChunkList.get(lastIndex), valueChunkList);
      currentIsAligned = true;
      lastMarker = marker;
      return true;
    }
    return false;
  }
}
