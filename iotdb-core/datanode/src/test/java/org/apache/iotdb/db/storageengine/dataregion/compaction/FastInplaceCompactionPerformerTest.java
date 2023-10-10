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

package org.apache.iotdb.db.storageengine.dataregion.compaction;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.InPlaceFastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.inplace.InPlaceCrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.validator.CompactionValidator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.CompactionEstimateUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class FastInplaceCompactionPerformerTest extends AbstractCompactionTest {

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
  }

  @Test
  public void testOneDeviceOneNonAlignedSeriesWithNonOverlapFiles() throws IOException {
    List<TsFileResource> seqFiles = new ArrayList<>();
    List<TsFileResource> unseqFiles = new ArrayList<>();
    TsFileResource seqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(10, 20)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(21, 24)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);


    seqFiles.add(seqResource1);
    unseqFiles.add(unseqResource1);
    tsFileManager.addAll(seqFiles, true);
    tsFileManager.addAll(unseqFiles, false);

    CompactionTaskManager.getInstance().start();
    InPlaceCrossSpaceCompactionTask t =
        new InPlaceCrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqFiles,
            unseqFiles,
            new InPlaceFastCompactionPerformer(),
            new AtomicInteger(0),
            100,
            0);
    t.checkValidAndSetMerging();
    Assert.assertTrue(t.start());
    validateCompactionResult(t.getSelectedSequenceFiles(), t.getSelectedUnsequenceFiles(), tsFileManager.getTsFileList(true));
    File targetFile = tsFileManager.getTsFileList(true).get(0).getTsFile();
    Assert.assertTrue(new File(targetFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).exists());
    assertDeviceTimeRange(tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 10, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertMeasurementTimeRange(reader, "root.testsg.d1", "s1", 10, 24);
    }
  }

  @Test
  public void testOneDeviceOneNonAlignedSeriesWithOverlapFiles() throws IOException {
    List<TsFileResource> seqFiles = new ArrayList<>();
    List<TsFileResource> unseqFiles = new ArrayList<>();
    TsFileResource seqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(10, 20)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(11, 24)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);


    seqFiles.add(seqResource1);
    unseqFiles.add(unseqResource1);
    tsFileManager.addAll(seqFiles, true);
    tsFileManager.addAll(unseqFiles, false);

    CompactionTaskManager.getInstance().start();
    InPlaceCrossSpaceCompactionTask t =
        new InPlaceCrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqFiles,
            unseqFiles,
            new InPlaceFastCompactionPerformer(),
            new AtomicInteger(0),
            100,
            0);
    t.checkValidAndSetMerging();
    Assert.assertTrue(t.start());
    validateCompactionResult(t.getSelectedSequenceFiles(), t.getSelectedUnsequenceFiles(), tsFileManager.getTsFileList(true));
    File targetFile = tsFileManager.getTsFileList(true).get(0).getTsFile();
    Assert.assertTrue(new File(targetFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).exists());
    assertDeviceTimeRange(tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 10, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertMeasurementTimeRange(reader, "root.testsg.d1", "s1", 10, 24);
    }

  }

  @Test
  public void testOneDeviceDifferentNonAlignedSeriesWithNonOverlapFiles() throws IOException {
    List<TsFileResource> seqFiles = new ArrayList<>();
    List<TsFileResource> unseqFiles = new ArrayList<>();
    TsFileResource seqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(10, 20)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s2",
            new TimeRange[] {new TimeRange(21, 24)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);


    seqFiles.add(seqResource1);
    unseqFiles.add(unseqResource1);
    tsFileManager.addAll(seqFiles, true);
    tsFileManager.addAll(unseqFiles, false);

    CompactionTaskManager.getInstance().start();
    InPlaceCrossSpaceCompactionTask t =
        new InPlaceCrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqFiles,
            unseqFiles,
            new InPlaceFastCompactionPerformer(),
            new AtomicInteger(0),
            100,
            0);
    t.checkValidAndSetMerging();
    Assert.assertTrue(t.start());
    validateCompactionResult(t.getSelectedSequenceFiles(), t.getSelectedUnsequenceFiles(), tsFileManager.getTsFileList(true));
    File targetFile = tsFileManager.getTsFileList(true).get(0).getTsFile();
    Assert.assertTrue(new File(targetFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).exists());
    assertDeviceTimeRange(tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 10, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertMeasurementTimeRange(reader, "root.testsg.d1", "s1", 10, 20);
      assertMeasurementTimeRange(reader, "root.testsg.d1", "s2", 21, 24);
    }
  }

  @Test
  public void testDifferentDeviceNonAlignedSeriesWithNonOverlapFiles() throws IOException {
    List<TsFileResource> seqFiles = new ArrayList<>();
    List<TsFileResource> unseqFiles = new ArrayList<>();
    TsFileResource seqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(10, 20)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d2",
            "s1",
            new TimeRange[] {new TimeRange(21, 24)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);


    seqFiles.add(seqResource1);
    unseqFiles.add(unseqResource1);
    tsFileManager.addAll(seqFiles, true);
    tsFileManager.addAll(unseqFiles, false);

    CompactionTaskManager.getInstance().start();
    InPlaceCrossSpaceCompactionTask t =
        new InPlaceCrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqFiles,
            unseqFiles,
            new InPlaceFastCompactionPerformer(),
            new AtomicInteger(0),
            100,
            0);
    t.checkValidAndSetMerging();
    Assert.assertTrue(t.start());
    validateCompactionResult(t.getSelectedSequenceFiles(), t.getSelectedUnsequenceFiles(), tsFileManager.getTsFileList(true));
    File targetFile = tsFileManager.getTsFileList(true).get(0).getTsFile();
    Assert.assertFalse(tsFileManager.getTsFileList(true).get(0).getModFile().exists());
    assertDeviceTimeRange(tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 10, 20);
    assertDeviceTimeRange(tsFileManager.getTsFileList(true).get(0), "root.testsg.d2", 21, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertMeasurementTimeRange(reader, "root.testsg.d1", "s1", 10, 20);
      assertMeasurementTimeRange(reader, "root.testsg.d2", "s1", 21, 24);
    }
  }

  @Test
  public void testOneDeviceOneNonAlignedSeriesWithMultiFiles() throws IOException {
    List<TsFileResource> seqFiles = new ArrayList<>();
    List<TsFileResource> unseqFiles = new ArrayList<>();
    TsFileResource seqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(10, 20)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    TsFileResource seqResource2 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(30, 50)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource2.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);

    TsFileResource seqResource3 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(70, 80)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource3.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);

    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(21, 24)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);

    TsFileResource unseqResource2 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(25, 60)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            false);
    unseqResource2.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);

    TsFileResource unseqResource3 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(90, 100)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            false);
    unseqResource3.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);

    seqFiles.add(seqResource1);
    seqFiles.add(seqResource2);
    seqFiles.add(seqResource3);
    unseqFiles.add(unseqResource1);
    unseqFiles.add(unseqResource2);
    unseqFiles.add(unseqResource3);

    CompactionTaskManager.getInstance().start();

    InPlaceCrossSpaceCompactionTask t =
        new InPlaceCrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqFiles,
            unseqFiles,
            new InPlaceFastCompactionPerformer(),
            new AtomicInteger(0),
            0,
            0);
    t.checkValidAndSetMerging();
    Assert.assertTrue(t.start());
    validateCompactionResult(t.getSelectedSequenceFiles(), t.getSelectedUnsequenceFiles(), tsFileManager.getTsFileList(true));
    List<TsFileResource> targetFiles = tsFileManager.getTsFileList(true);
    assertDeviceTimeRange(targetFiles.get(0), "root.testsg.d1", 10, 20);
    assertDeviceTimeRange(targetFiles.get(1), "root.testsg.d1", 21, 50);
    assertDeviceTimeRange(targetFiles.get(2), "root.testsg.d1", 51, 100);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFiles.get(0).getTsFilePath())) {
      assertMeasurementTimeRange(reader, "root.testsg.d1", "s1", 10, 20);
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFiles.get(1).getTsFilePath())) {
      assertMeasurementTimeRange(reader, "root.testsg.d1", "s1", 21, 50);
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFiles.get(2).getTsFilePath())) {
      assertMeasurementTimeRange(reader, "root.testsg.d1", "s1", 51, 100);
    }
  }

  @Test
  public void test2() {
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(
            "/home/sww/source-codes/iotdb/iotdb-core/datanode/target/data/sequence/root.testsg/0/0/0-18-0-0.tsfile")) {
      final Map<String, List<ChunkMetadata>> deviceChunkMetadataMap =
          reader.readChunkMetadataInDevice("root.testsg.d1");
      for (Map.Entry<String, List<ChunkMetadata>> measurementChunkMetadataListEntry :
          deviceChunkMetadataMap.entrySet()) {
        System.out.println(measurementChunkMetadataListEntry.getKey());
        for (ChunkMetadata chunkMetadata : measurementChunkMetadataListEntry.getValue()) {
          System.out.println(chunkMetadata);

          final Chunk chunk = reader.readMemChunk(chunkMetadata);
          ChunkReader chunkReader = new ChunkReader(chunk);

          final PageHeader pageHeader =
              PageHeader.deserializeFrom(chunk.getData(), chunkMetadata.getStatistics());
          final ByteBuffer pageBuffer = chunkReader.readPageDataWithoutUncompressing(pageHeader);

          final TsBlock tsBlock = chunkReader.readPageData(pageHeader, pageBuffer);
          final TsBlock.TsBlockRowIterator tsBlockRowIterator = tsBlock.getTsBlockRowIterator();

          long sum = 0;
          int intSum = 0;
          while (tsBlockRowIterator.hasNext()) {
            final Object[] next = tsBlockRowIterator.next();
            System.out.print(next[0]);
            System.out.print("  ");
            System.out.println(next[1]);
            sum += (Integer) next[0];
            intSum += (Integer) next[0];
          }
          System.out.println(sum);
          System.out.println(intSum);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private TsFileResource generateSingleNonAlignedSeriesFile(
      String device,
      String measurement,
      TimeRange[] chunkTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType,
      boolean isSeq)
      throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResource(isSeq);
    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    writer1.startChunkGroup(device);
    writer1.generateSimpleNonAlignedSeriesToCurrentDevice(
        measurement, chunkTimeRanges, encoding, compressionType);
    writer1.endChunkGroup();
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }

  private void validateCompactionResult(List<TsFileResource> sourceSeqFiles, List<TsFileResource> sourceUnSeqFiles, List<TsFileResource> targetFiles) {
    Assert.assertEquals(SystemInfo.getInstance().getCompactionFileNumCost().get(), 0);
    Assert.assertEquals(SystemInfo.getInstance().getCompactionMemoryCost().get(), 0L);
    for (TsFileResource resource : sourceSeqFiles) {
      String tsFilePath = resource.getTsFilePath();
      Assert.assertEquals(resource.getStatus(), TsFileResourceStatus.DELETED);
      Assert.assertFalse(new File(tsFilePath + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(new File(tsFilePath + ModificationFile.FILE_SUFFIX).exists());
      Assert.assertFalse(new File(tsFilePath + ModificationFile.COMPACTION_FILE_SUFFIX).exists());
      Assert.assertFalse(new File(tsFilePath + IoTDBConstant.IN_PLACE_COMPACTION_TEMP_METADATA_FILE_SUFFIX).exists());
      Assert.assertTrue(resource.tryWriteLock());
      resource.writeUnlock();
    }
    for (TsFileResource resource : sourceUnSeqFiles) {
      String tsFilePath = resource.getTsFilePath();
      Assert.assertEquals(resource.getStatus(), TsFileResourceStatus.DELETED);
      Assert.assertFalse(new File(tsFilePath + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(new File(tsFilePath + ModificationFile.FILE_SUFFIX).exists());
      Assert.assertFalse(new File(tsFilePath + ModificationFile.COMPACTION_FILE_SUFFIX).exists());
      Assert.assertTrue(resource.tryWriteLock());
      resource.writeUnlock();
    }
    // validate target files
    for (TsFileResource resource : targetFiles) {
      String tsFilePath = resource.getTsFilePath();
      Assert.assertEquals(resource.getStatus(), TsFileResourceStatus.NORMAL);
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(new File(tsFilePath + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertTrue(CompactionValidator.getInstance().validateSingleTsFile(resource));
      Assert.assertTrue(resource.tryWriteLock());
      resource.writeUnlock();
    }
    tsFileManager.writeLock("test");
    tsFileManager.writeUnlock();
  }

  private void assertMeasurementTimeRange(TsFileSequenceReader reader, String deviceId, String measurementId, long startTime, long endTime) throws IOException {
    List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(new Path(deviceId, measurementId, true));
    long startTimeInMetadata = Long.MAX_VALUE, endTimeInMetadata = Long.MIN_VALUE;
    for (ChunkMetadata chunkMetadata : chunkMetadataList) {
      startTimeInMetadata = Math.min(startTimeInMetadata, chunkMetadata.getStartTime());
      endTimeInMetadata = Math.max(endTimeInMetadata, chunkMetadata.getEndTime());
    }
    Assert.assertEquals(startTime, startTimeInMetadata);
    Assert.assertEquals(endTime, endTimeInMetadata);
  }

  private void assertDeviceTimeRange(TsFileResource resource, String deviceId, long startTime, long endTime) {
    Assert.assertEquals(resource.getStartTime(deviceId), startTime);
    Assert.assertEquals(resource.getEndTime(deviceId), endTime);
  }
}
