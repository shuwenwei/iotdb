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

package org.apache.iotdb.db.storageengine.dataregion.compaction.utils;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.reader.CompactingTsFileInput;

import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CompactingTsFileInputTest extends AbstractCompactionTest {
  @Test
  public void test1()
      throws IOException, MetadataException, WriteProcessException, URISyntaxException {
    TsFileResource resource = createEmptyFileAndResource(true);
    File file = resource.getTsFile();
    String metadataFilePath =
        file.getAbsolutePath() + IoTDBConstant.IN_PLACE_COMPACTION_TEMP_METADATA_FILE_SUFFIX;
    new File(metadataFilePath).createNewFile();
    long splitLength;
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
      writer.startChunkGroup("d0");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[] {new TimeRange(1000, 2000)},
          TSEncoding.PLAIN,
          CompressionType.UNCOMPRESSED);
      writer.endChunkGroup();
      splitLength = file.length();
      writer.endFile();
    }

    BufferedInputStream bis =
        new BufferedInputStream(Files.newInputStream(Paths.get(file.getAbsolutePath())));
    bis.skip(splitLength);
    BufferedOutputStream os =
        new BufferedOutputStream(Files.newOutputStream(Paths.get(metadataFilePath)));
    byte[] data = new byte[1024];
    int readSize;
    while ((readSize = bis.read(data)) != -1) {
      os.write(data, 0, readSize);
    }
    os.flush();
    new RandomAccessFile(file, "rw").setLength(splitLength);

    Path dataPath = resource.getTsFile().toPath();
    Path metadataPath =
        new File(
                resource.getTsFile().getAbsolutePath()
                    + IoTDBConstant.IN_PLACE_COMPACTION_TEMP_METADATA_FILE_SUFFIX)
            .toPath();
    CompactingTsFileInput compactingTsFileInput = new CompactingTsFileInput(dataPath, metadataPath);

    try (TsFileSequenceReader reader = new TsFileSequenceReader(compactingTsFileInput)) {
      List<String> allDevices = reader.getAllDevices();
      for (String device : allDevices) {
        Iterator<Map<String, List<ChunkMetadata>>> measurementChunkMetadataListMapIterator =
            reader.getMeasurementChunkMetadataListMapIterator(device);
        while (measurementChunkMetadataListMapIterator.hasNext()) {
          Map<String, List<ChunkMetadata>> measurementChunkMetadataListmap =
              measurementChunkMetadataListMapIterator.next();
          for (Map.Entry<String, List<ChunkMetadata>> measurementChunkMetadataList :
              measurementChunkMetadataListmap.entrySet()) {
            System.out.println(measurementChunkMetadataList.getKey());
            System.out.println(measurementChunkMetadataList.getValue());
            for (ChunkMetadata chunkMetadata : measurementChunkMetadataList.getValue()) {
              Chunk chunk = reader.readMemChunk(chunkMetadata);
              ChunkHeader header = chunk.getHeader();
              System.out.println(header);
            }
          }
        }
      }
    }
  }
}
