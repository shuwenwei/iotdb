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

import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileDeviceIterator;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class TsFileStatisticReaderV2 implements Closeable {

  private final TsFileSequenceReader reader;

  public TsFileStatisticReaderV2(String filePath) throws IOException {
    reader = new TsFileSequenceReader(filePath);
  }

  public TsFileDeviceIterator getDeviceIterator() throws IOException {
    return reader.getAllDevicesIteratorWithIsAligned();
  }

  public Iterator<List<ChunkMetadata>> getDeviceChunkMetadataListIterator(String device)
      throws IOException {
    return new DeviceChunkMetadataListIterator(reader, device);
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  public static class DeviceChunkMetadataListIterator implements Iterator<List<ChunkMetadata>> {

    Iterator<Map<String, List<ChunkMetadata>>> measurementChunkMetadataListMapIterator;
    Iterator<Map.Entry<String, List<ChunkMetadata>>> measurementChunkMetadataListIterator;
    List<ChunkMetadata> chunkMetadataList;

    private DeviceChunkMetadataListIterator(TsFileSequenceReader reader, String device)
        throws IOException {
      measurementChunkMetadataListMapIterator =
          reader.getMeasurementChunkMetadataListMapIterator(device);
      while (measurementChunkMetadataListMapIterator.hasNext()) {
        measurementChunkMetadataListIterator =
            measurementChunkMetadataListMapIterator.next().entrySet().iterator();
        if (measurementChunkMetadataListIterator.hasNext()) {
          chunkMetadataList = measurementChunkMetadataListIterator.next().getValue();
          return;
        }
      }
    }

    @Override
    public boolean hasNext() {
      return chunkMetadataList != null;
    }

    @Override
    public List<ChunkMetadata> next() {
      if (chunkMetadataList == null) {
        throw new NoSuchElementException();
      }
      // calculate next value
      List<ChunkMetadata> result = chunkMetadataList;
      if (measurementChunkMetadataListIterator.hasNext()) {
        chunkMetadataList = measurementChunkMetadataListIterator.next().getValue();
        return result;
      }
      while (measurementChunkMetadataListMapIterator.hasNext()) {
        measurementChunkMetadataListIterator =
            measurementChunkMetadataListMapIterator.next().entrySet().iterator();
        if (measurementChunkMetadataListIterator.hasNext()) {
          chunkMetadataList = measurementChunkMetadataListIterator.next().getValue();
          return result;
        }
      }

      chunkMetadataList = null;
      return result;
    }
  }
}
