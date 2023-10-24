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
package org.apache.iotdb.db.storageengine.dataregion.tsfile;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

public class TsFileResourceTest {
  private final File file =
      new File(
          TsFileNameGenerator.generateNewTsFilePath(TestConstant.BASE_OUTPUT_PATH, 1, 1, 1, 1));
  private final TsFileResource tsFileResource = new TsFileResource(file);
  private final Map<String, Integer> deviceToIndex = new HashMap<>();
  private final long[] startTimes = new long[DEVICE_NUM];
  private final long[] endTimes = new long[DEVICE_NUM];
  private static final int DEVICE_NUM = 100;

  @Before
  public void setUp() {
    IntStream.range(0, DEVICE_NUM).forEach(i -> deviceToIndex.put("root.sg.d" + i, i));
    DeviceTimeIndex deviceTimeIndex = new DeviceTimeIndex(deviceToIndex, startTimes, endTimes);
    IntStream.range(0, DEVICE_NUM)
        .forEach(
            i -> {
              deviceTimeIndex.updateStartTime("root.sg.d" + i, i);
              deviceTimeIndex.updateEndTime("root.sg.d" + i, i + 1);
            });
    tsFileResource.setTimeIndex(deviceTimeIndex);
    tsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
  }

  @After
  public void tearDown() throws IOException {
    // clean fake file
    if (file.exists()) {
      FileUtils.delete(file);
    }
    File resourceFile = new File(file.getName() + TsFileResource.RESOURCE_SUFFIX);
    if (resourceFile.exists()) {
      FileUtils.delete(resourceFile);
    }
  }

  @Test
  public void testSerializeAndDeserialize() throws IOException {
    tsFileResource.serialize();
    TsFileResource derTsFileResource = new TsFileResource(file);
    derTsFileResource.deserialize();
    Assert.assertEquals(tsFileResource, derTsFileResource);
  }

  @Test
  public void testHardLinkWithNormalFile() throws IOException {
    File tsFile =
        new File(
            TsFileNameGenerator.generateNewTsFilePath(TestConstant.BASE_OUTPUT_PATH, 1, 1, 1, 1));
    writeBytesToFile(tsFile, 100);
    TsFileResource tsFileResource = new TsFileResource(tsFile);
    tsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
    File hardLinkFile = new File(tsFile.getAbsoluteFile() + ".link");
    tsFileResource.hardLinkOrCopyTsFileTo(hardLinkFile.toPath());
    Assert.assertEquals(100, hardLinkFile.length());

    Files.deleteIfExists(tsFile.toPath());
    Files.deleteIfExists(hardLinkFile.toPath());
  }

  @Test
  public void testHardLinkUsingCopyFile() throws IOException {
    File tsFile =
        new File(
            TsFileNameGenerator.generateNewTsFilePath(TestConstant.BASE_OUTPUT_PATH, 1, 1, 1, 1));
    writeBytesToFile(tsFile, 100);
    TsFileResource tsFileResource = new TsFileResource(tsFile);
    tsFileResource.setStatusForTest(TsFileResourceStatus.COMPACTING);
    File hardLinkFile = new File(tsFile.getAbsoluteFile() + ".link");
    tsFileResource.hardLinkOrCopyTsFileTo(hardLinkFile.toPath());
    Assert.assertEquals(100, hardLinkFile.length());

    Files.deleteIfExists(tsFile.toPath());
    Files.deleteIfExists(hardLinkFile.toPath());
  }

  @Test
  @Ignore
  /**
   * This test is used to test transferring big file whose size is larger than 2GB. We ignored this
   * test to avoid long time running
   */
  public void testHardLinkWith3GSplitFile() throws IOException {
    File tsFile =
        new File(
            TsFileNameGenerator.generateNewTsFilePath(TestConstant.BASE_OUTPUT_PATH, 1, 1, 1, 1));
    writeBytesToFile(tsFile, 1024L * 1024 * 1024 * 3 + 1024);
    TsFileResource tsFileResource = new TsFileResource(tsFile);
    tsFileResource.setStatusForTest(TsFileResourceStatus.SPLIT_DURING_COMPACTING);
    tsFileResource.setDataSize(1024L * 1024 * 1024 * 3);

    File metaFile =
        new File(
            TsFileNameGenerator.generateNewTsFilePath(TestConstant.BASE_OUTPUT_PATH, 1, 1, 1, 1)
                + IoTDBConstant.IN_PLACE_COMPACTION_TEMP_METADATA_FILE_SUFFIX);
    writeBytesToFile(metaFile, 100);

    File hardLinkFile = new File(tsFile.getAbsoluteFile() + ".link");
    tsFileResource.hardLinkOrCopyTsFileTo(hardLinkFile.toPath());
    Assert.assertEquals(1024L * 1024 * 1024 * 3 + 100, hardLinkFile.length());
    Files.deleteIfExists(tsFile.toPath());
    Files.deleteIfExists(hardLinkFile.toPath());
  }

  @Test
  public void testHardLinkWith3MSplitFile() throws IOException {
    File tsFile =
        new File(
            TsFileNameGenerator.generateNewTsFilePath(TestConstant.BASE_OUTPUT_PATH, 1, 1, 1, 1));
    writeBytesToFile(tsFile, 1024L * 1024 * 3 + 1024);
    TsFileResource tsFileResource = new TsFileResource(tsFile);
    tsFileResource.setStatusForTest(TsFileResourceStatus.SPLIT_DURING_COMPACTING);
    tsFileResource.setDataSize(1024L * 1024 * 3);

    File metaFile =
        new File(
            TsFileNameGenerator.generateNewTsFilePath(TestConstant.BASE_OUTPUT_PATH, 1, 1, 1, 1)
                + IoTDBConstant.IN_PLACE_COMPACTION_TEMP_METADATA_FILE_SUFFIX);
    writeBytesToFile(metaFile, 100);

    File hardLinkFile = new File(tsFile.getAbsoluteFile() + ".link");
    tsFileResource.hardLinkOrCopyTsFileTo(hardLinkFile.toPath());
    Assert.assertEquals(1024L * 1024 * 3 + 100, hardLinkFile.length());
    Files.deleteIfExists(tsFile.toPath());
    Files.deleteIfExists(hardLinkFile.toPath());
  }

  @Test
  public void testHardLinkWith3MSplitFileWithoutDirtyData() throws IOException {
    File tsFile =
        new File(
            TsFileNameGenerator.generateNewTsFilePath(TestConstant.BASE_OUTPUT_PATH, 1, 1, 1, 1));
    writeBytesToFile(tsFile, 1024L * 1024 * 3);
    TsFileResource tsFileResource = new TsFileResource(tsFile);
    tsFileResource.setStatusForTest(TsFileResourceStatus.SPLIT_DURING_COMPACTING);
    tsFileResource.setDataSize(1024L * 1024 * 3);

    File metaFile =
        new File(
            TsFileNameGenerator.generateNewTsFilePath(TestConstant.BASE_OUTPUT_PATH, 1, 1, 1, 1)
                + IoTDBConstant.IN_PLACE_COMPACTION_TEMP_METADATA_FILE_SUFFIX);
    writeBytesToFile(metaFile, 100);

    File hardLinkFile = new File(tsFile.getAbsoluteFile() + ".link");
    tsFileResource.hardLinkOrCopyTsFileTo(hardLinkFile.toPath());
    Assert.assertEquals(1024L * 1024 * 3 + 100, hardLinkFile.length());
    Files.deleteIfExists(tsFile.toPath());
    Files.deleteIfExists(hardLinkFile.toPath());
  }

  private void writeBytesToFile(File file, long bytesLength) throws IOException {
    try (FileOutputStream fos = new FileOutputStream(file);
        FileChannel fileChannel = fos.getChannel()) {
      int bufferSize = 1024 * 1024;
      long leftSize = bytesLength;
      while (leftSize > 0) {
        byte[] data;
        if (leftSize > bufferSize) {
          data = new byte[bufferSize];
        } else {
          data = new byte[(int) leftSize];
        }
        for (int i = 0; i < data.length; i++) {
          data[i] = (byte) (i + 1);
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        fileChannel.write(buffer);
        leftSize -= data.length;
      }
    }
  }

  @Test
  public void testDegradeAndFileTimeIndex() {
    Assert.assertEquals(ITimeIndex.DEVICE_TIME_INDEX_TYPE, tsFileResource.getTimeIndexType());
    tsFileResource.degradeTimeIndex();
    Assert.assertEquals(ITimeIndex.FILE_TIME_INDEX_TYPE, tsFileResource.getTimeIndexType());
    Assert.assertEquals(deviceToIndex.keySet(), tsFileResource.getDevices());
    for (int i = 0; i < DEVICE_NUM; i++) {
      Assert.assertEquals(tsFileResource.getStartTime("root.sg1.d" + i), 0);
      Assert.assertEquals(tsFileResource.getEndTime("root.sg1.d" + i), DEVICE_NUM);
    }
  }
}
