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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.InPlaceFastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.inplace.InPlaceCrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.validator.CompactionValidator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionWorker;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.comparator.DefaultCompactionTaskComparatorImpl;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.db.utils.datastructure.FixedPriorityBlockingQueue;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FastInplaceCompactionPerformerTest extends AbstractCompactionTest {

  private final FixedPriorityBlockingQueue<AbstractCompactionTask> candidateCompactionTaskQueue =
      new FixedPriorityBlockingQueue<>(50, new DefaultCompactionTaskComparatorImpl());
  private final CompactionWorker worker = new CompactionWorker(0, candidateCompactionTaskQueue);

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
  }

  @Test
  public void testOneDeviceOneNonAlignedSeriesWithNonOverlappedFiles() throws IOException {
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
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    File targetFile = tsFileManager.getTsFileList(true).get(0).getTsFile();
    ModificationFile modsFile = tsFileManager.getTsFileList(true).get(0).getModFile();
    Assert.assertTrue(modsFile.exists());
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 10, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s1", 10, 24);
    }
  }

  @Test
  public void testOneDeviceAlignedSeriesWithNonOverlappedFiles() throws IOException, IllegalPathException {
    List<TsFileResource> seqFiles = new ArrayList<>();
    List<TsFileResource> unseqFiles = new ArrayList<>();
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d1",
            Arrays.asList("s1", "s2"),
            new TimeRange[] {new TimeRange(10, 20)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d1",
            Arrays.asList("s1", "s2"),
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
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    File targetFile = tsFileManager.getTsFileList(true).get(0).getTsFile();
    ModificationFile modsFile = tsFileManager.getTsFileList(true).get(0).getModFile();
    Assert.assertTrue(modsFile.exists());
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 10, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s1", 10, 24);
      assertAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s2", 10, 24);
    }
  }

  @Test
  public void testOneSourceSeqFileOverlappedNonAlignedDeviceHasPartialDeletion()
      throws IOException, IllegalPathException {
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
    seqResource1
        .getModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s1"), Long.MAX_VALUE, 0, 15));
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
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    File targetFile = tsFileManager.getTsFileList(true).get(0).getTsFile();
    ModificationFile modsFile = tsFileManager.getTsFileList(true).get(0).getModFile();
    Assert.assertTrue(modsFile.exists());
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 16, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s1", 16, 24);
    }
  }


  @Test
  public void testOneSourceSeqFileOverlappedAlignedDeviceHasPartialDeletion()
      throws IOException, IllegalPathException {
    List<TsFileResource> seqFiles = new ArrayList<>();
    List<TsFileResource> unseqFiles = new ArrayList<>();
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d1",
            Arrays.asList("s1", "s2"),
            new TimeRange[] {new TimeRange(10, 20)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource1
        .getModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s1"), Long.MAX_VALUE, 0, 15));
    seqResource1
        .getModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s2"), Long.MAX_VALUE, 0, 15));
    seqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d1",
            Arrays.asList("s1", "s2"),
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
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    File targetFile = tsFileManager.getTsFileList(true).get(0).getTsFile();
    ModificationFile modsFile = tsFileManager.getTsFileList(true).get(0).getModFile();
    Assert.assertTrue(modsFile.exists());
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 16, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s1", 16, 24);
      assertNonAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s2", 16, 24);
    }
  }

  @Test
  public void testOneSourceSeqFileOverlappedNonAlignedDeviceHasFullDeletion1()
      throws IOException, IllegalPathException {
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
    seqResource1
        .getModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s1"), Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE));
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
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    File targetFile = tsFileManager.getTsFileList(true).get(0).getTsFile();
    ModificationFile modsFile = tsFileManager.getTsFileList(true).get(0).getModFile();
    Assert.assertTrue(modsFile.exists());
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 21, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s1", 21, 24);
    }
  }

  @Test
  public void testOneSourceSeqFileOverlappedNonAlignedDeviceHasFullDeletion2()
      throws IOException, IllegalPathException {
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
    seqResource1
        .getModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s1"), Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE));
    seqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(15, 24)},
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
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    File targetFile = tsFileManager.getTsFileList(true).get(0).getTsFile();
    ModificationFile modsFile = tsFileManager.getTsFileList(true).get(0).getModFile();
    Assert.assertTrue(modsFile.exists());
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 15, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s1", 15, 24);
    }
  }

  @Test
  public void testOneSourceSeqFileOverlappedAlignedDeviceHasFullDeletion1()
      throws IOException, IllegalPathException {
    List<TsFileResource> seqFiles = new ArrayList<>();
    List<TsFileResource> unseqFiles = new ArrayList<>();
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d1",
            Arrays.asList("s1", "s2"),
            new TimeRange[] {new TimeRange(10, 20)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource1
        .getModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s1"), Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE));
    seqResource1
        .getModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s2"), Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE));
    seqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d1",
            Arrays.asList("s1", "s2"),
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
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    File targetFile = tsFileManager.getTsFileList(true).get(0).getTsFile();
    ModificationFile modsFile = tsFileManager.getTsFileList(true).get(0).getModFile();
    Assert.assertTrue(modsFile.exists());
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 21, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s1", 21, 24);
      assertNonAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s2", 21, 24);
    }
  }

  @Test
  public void testOneSourceSeqFileOverlappedAlignedDeviceHasFullDeletion2()
      throws IOException, IllegalPathException {
    List<TsFileResource> seqFiles = new ArrayList<>();
    List<TsFileResource> unseqFiles = new ArrayList<>();
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d1",
            Arrays.asList("s1", "s2"),
            new TimeRange[] {new TimeRange(10, 20)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource1
        .getModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s1"), Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE));
    seqResource1
        .getModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s2"), Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE));
    seqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d1",
            Arrays.asList("s1", "s2"),
            new TimeRange[] {new TimeRange(15, 24)},
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
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    File targetFile = tsFileManager.getTsFileList(true).get(0).getTsFile();
    ModificationFile modsFile = tsFileManager.getTsFileList(true).get(0).getModFile();
    Assert.assertTrue(modsFile.exists());
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 15, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s1", 15, 24);
      assertNonAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s2", 15, 24);
    }
  }

  @Test
  public void testOneSourceSeqFileOverlappedNonAlignedDeviceHasPartialDeletionAndCompactionMods()
      throws IOException, IllegalPathException {
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
    seqResource1
        .getModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s1"), seqResource1.getTsFileSize(), 0, 15));
    seqResource1
        .getCompactionModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s1"), seqResource1.getTsFileSize(), 0, 15));
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
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    File targetFile = tsFileManager.getTsFileList(true).get(0).getTsFile();
    ModificationFile modsFile = tsFileManager.getTsFileList(true).get(0).getModFile();
    Assert.assertTrue(modsFile.exists());
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 16, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s1", 16, 24);
    }
  }

  @Test
  public void testOneSourceSeqFileOverlappedAlignedDeviceHasPartialDeletionAndCompactionMods()
      throws IOException, IllegalPathException {
    List<TsFileResource> seqFiles = new ArrayList<>();
    List<TsFileResource> unseqFiles = new ArrayList<>();
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d1",
            Arrays.asList("s1", "s2"),
            new TimeRange[] {new TimeRange(10, 20)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource1
        .getModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s1"), seqResource1.getTsFileSize(), 0, 15));
    seqResource1
        .getModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s2"), seqResource1.getTsFileSize(), 0, 15));
    seqResource1
        .getCompactionModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s2"), seqResource1.getTsFileSize(), 0, 15));
    seqResource1
        .getCompactionModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s2"), seqResource1.getTsFileSize(), 0, 15));
    seqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d1",
            Arrays.asList("s1", "s2"),
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
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    File targetFile = tsFileManager.getTsFileList(true).get(0).getTsFile();
    ModificationFile modsFile = tsFileManager.getTsFileList(true).get(0).getModFile();
    Assert.assertTrue(modsFile.exists());
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 16, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s1", 16, 24);
      assertNonAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s2", 16, 24);
    }
  }


  @Test
  public void testOneSourceSeqFileOverlappedNonAlignedDeviceHasFullDeletionAndCompactionMods1()
      throws IOException, IllegalPathException {
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
    seqResource1
        .getCompactionModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s1"), seqResource1.getTsFileSize(), Long.MIN_VALUE, Long.MAX_VALUE));
    seqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(21, 24)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            false);
    unseqResource1
        .getCompactionModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s1"), seqResource1.getTsFileSize(), Long.MIN_VALUE, Long.MAX_VALUE));
    unseqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);

    seqFiles.add(seqResource1);
    unseqFiles.add(unseqResource1);
    tsFileManager.addAll(seqFiles, true);
    tsFileManager.addAll(unseqFiles, false);

    CompactionTaskManager.getInstance().start();
    InPlaceCrossSpaceCompactionTask t =
        new InPlaceCrossSpaceCompactionTask(
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    File targetFile = tsFileManager.getTsFileList(true).get(0).getTsFile();
    ModificationFile modsFile = tsFileManager.getTsFileList(true).get(0).getModFile();
    Assert.assertTrue(modsFile.exists());
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 10, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s1", Long.MAX_VALUE, Long.MIN_VALUE);
    }
  }

  @Test
  public void testOneSourceSeqFileOverlappedNonAlignedDeviceHasFullDeletionAndCompactionMods2()
      throws IOException, IllegalPathException {
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
    seqResource1
        .getCompactionModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s1"), Long.MAX_VALUE, 10, 20));
    seqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(15, 24)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            false);
    unseqResource1
        .getCompactionModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s1"), Long.MAX_VALUE, 10, 20));
    unseqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);

    seqFiles.add(seqResource1);
    unseqFiles.add(unseqResource1);
    tsFileManager.addAll(seqFiles, true);
    tsFileManager.addAll(unseqFiles, false);

    CompactionTaskManager.getInstance().start();
    InPlaceCrossSpaceCompactionTask t =
        new InPlaceCrossSpaceCompactionTask(
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    File targetFile = tsFileManager.getTsFileList(true).get(0).getTsFile();
    ModificationFile modsFile = tsFileManager.getTsFileList(true).get(0).getModFile();
    Assert.assertTrue(modsFile.exists());
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 10, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s1", 21, 24);
    }
  }

  @Test
  public void testOneSourceSeqFileOverlappedAlignedDeviceHasFullDeletionAndCompactionMods1()
      throws IOException, IllegalPathException {
    List<TsFileResource> seqFiles = new ArrayList<>();
    List<TsFileResource> unseqFiles = new ArrayList<>();
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d1",
            Arrays.asList("s1", "s2"),
            new TimeRange[] {new TimeRange(10, 20)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource1
        .getCompactionModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s1"), seqResource1.getTsFileSize(), Long.MIN_VALUE, Long.MAX_VALUE));
    seqResource1
        .getCompactionModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s2"), seqResource1.getTsFileSize(), Long.MIN_VALUE, Long.MAX_VALUE));
    seqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d1",
            Arrays.asList("s1", "s2"),
            new TimeRange[] {new TimeRange(21, 24)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            false);
    unseqResource1
        .getCompactionModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s1"), unseqResource1.getTsFileSize(), Long.MIN_VALUE, Long.MAX_VALUE));
    unseqResource1
        .getCompactionModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s2"), unseqResource1.getTsFileSize(), Long.MIN_VALUE, Long.MAX_VALUE));
    unseqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);

    seqFiles.add(seqResource1);
    unseqFiles.add(unseqResource1);
    tsFileManager.addAll(seqFiles, true);
    tsFileManager.addAll(unseqFiles, false);

    CompactionTaskManager.getInstance().start();
    InPlaceCrossSpaceCompactionTask t =
        new InPlaceCrossSpaceCompactionTask(
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    File targetFile = tsFileManager.getTsFileList(true).get(0).getTsFile();
    ModificationFile modsFile = tsFileManager.getTsFileList(true).get(0).getModFile();
    Assert.assertTrue(modsFile.exists());
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 10, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s1", Long.MAX_VALUE, Long.MIN_VALUE);
      assertAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s2", Long.MAX_VALUE, Long.MIN_VALUE);
    }
  }

  @Test
  public void testOneSourceSeqFileOverlappedAlignedDeviceHasFullDeletionAndCompactionMods2()
      throws IOException, IllegalPathException {
    List<TsFileResource> seqFiles = new ArrayList<>();
    List<TsFileResource> unseqFiles = new ArrayList<>();
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d1",
            Arrays.asList("s1", "s2"),
            new TimeRange[] {new TimeRange(10, 20)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource1
        .getCompactionModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s1"), seqResource1.getTsFileSize(), 10, 20));
    seqResource1
        .getCompactionModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s2"), seqResource1.getTsFileSize(), 10, 20));
    seqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d1",
            Arrays.asList("s1", "s2"),
            new TimeRange[] {new TimeRange(15, 24)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            false);
    unseqResource1
        .getCompactionModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s1"), unseqResource1.getTsFileSize(), 10, 20));
    unseqResource1
        .getCompactionModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s2"), unseqResource1.getTsFileSize(), 10, 20));
    unseqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);

    seqFiles.add(seqResource1);
    unseqFiles.add(unseqResource1);
    tsFileManager.addAll(seqFiles, true);
    tsFileManager.addAll(unseqFiles, false);

    CompactionTaskManager.getInstance().start();
    InPlaceCrossSpaceCompactionTask t =
        new InPlaceCrossSpaceCompactionTask(
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    File targetFile = tsFileManager.getTsFileList(true).get(0).getTsFile();
    ModificationFile modsFile = tsFileManager.getTsFileList(true).get(0).getModFile();
    Assert.assertTrue(modsFile.exists());
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 10, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s1", 21, 24);
      assertAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s2", 21, 24);
    }
  }

  @Test
  public void testOneSourceSeqFileNonOverlappedNonAlignedDeviceHasPartialDeletion()
      throws IOException, IllegalPathException {
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
    seqResource1
        .getModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s1"), Long.MAX_VALUE, 0, 15));
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
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    File targetFile = tsFileManager.getTsFileList(true).get(0).getTsFile();
    ModificationFile modsFile = tsFileManager.getTsFileList(true).get(0).getModFile();
    Assert.assertTrue(modsFile.exists());
    // the time range in .resource file is not updated
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 10, 20);
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d2", 21, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s1", 16, 20);
      assertNonAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d2", "s1", 21, 24);
    }
  }

  @Test
  public void testOneSourceSeqFileNonOverlappedAlignedDeviceHasPartialDeletion()
      throws IOException, IllegalPathException {
    List<TsFileResource> seqFiles = new ArrayList<>();
    List<TsFileResource> unseqFiles = new ArrayList<>();
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d1",
            Arrays.asList("s1", "s2"),
            new TimeRange[] {new TimeRange(10, 20)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource1
        .getModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s1"), Long.MAX_VALUE, 0, 15));
    seqResource1
        .getModFile()
        .write(new Deletion(new PartialPath("root.testsg.d1", "s2"), Long.MAX_VALUE, 0, 15));
    seqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d2",
            Arrays.asList("s1", "s2"),
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
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    File targetFile = tsFileManager.getTsFileList(true).get(0).getTsFile();
    ModificationFile modsFile = tsFileManager.getTsFileList(true).get(0).getModFile();
    Assert.assertTrue(modsFile.exists());
    // the time range in .resource file is not updated
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 10, 20);
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d2", 21, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s1", 16, 20);
      assertAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d1", "s2", 16, 20);

      assertAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d2", "s1", 21, 24);
      assertAlignedMeasurementTimeRangeInTsFile(reader, modsFile, "root.testsg.d2", "s2", 21, 24);
    }
  }

  @Test
  public void testDifferentDeviceAlignedSeriesWithNonOverlappedFiles() throws IOException, IllegalPathException {
    List<TsFileResource> seqFiles = new ArrayList<>();
    List<TsFileResource> unseqFiles = new ArrayList<>();
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d1",
            Arrays.asList("s1", "s2"),
            new TimeRange[] {new TimeRange(10, 20)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d2",
            Arrays.asList("s1", "s2"),
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
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    TsFileResource targetFileResource = tsFileManager.getTsFileList(true).get(0);
    File targetFile = targetFileResource.getTsFile();
    Assert.assertFalse(
        new File(targetFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).exists());
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 10, 20);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertAlignedMeasurementTimeRangeInTsFile(
          reader, targetFileResource.getModFile(), "root.testsg.d1", "s1", 10, 20);
      assertAlignedMeasurementTimeRangeInTsFile(
          reader, targetFileResource.getModFile(), "root.testsg.d2", "s2", 21, 24);
    }
  }

  @Test
  public void testOneDeviceOneNonAlignedSeriesWithOverlappedFiles() throws IOException {
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
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    TsFileResource targetFileResource = tsFileManager.getTsFileList(true).get(0);
    File targetFile = targetFileResource.getTsFile();
    Assert.assertTrue(
        new File(targetFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).exists());
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 10, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(
          reader, targetFileResource.getModFile(), "root.testsg.d1", "s1", 10, 24);
    }
  }

  @Test
  public void testOneDeviceDifferentNonAlignedSeriesWithNonOverlappedFiles() throws IOException {
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
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    TsFileResource targetFileResource = tsFileManager.getTsFileList(true).get(0);
    File targetFile = targetFileResource.getTsFile();
    Assert.assertTrue(
        new File(targetFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).exists());
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 10, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(
          reader, targetFileResource.getModFile(), "root.testsg.d1", "s1", 10, 20);
      assertNonAlignedMeasurementTimeRangeInTsFile(
          reader, targetFileResource.getModFile(), "root.testsg.d1", "s2", 21, 24);
    }
  }

  @Test
  public void testDifferentDeviceNonAlignedSeriesWithNonOverlappedFiles() throws IOException {
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
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    TsFileResource targetFileResource = tsFileManager.getTsFileList(true).get(0);
    File targetFile = targetFileResource.getTsFile();
    Assert.assertFalse(tsFileManager.getTsFileList(true).get(0).getModFile().exists());
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d1", 10, 20);
    assertDeviceTimeRangeInResource(
        tsFileManager.getTsFileList(true).get(0), "root.testsg.d2", 21, 24);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(
          reader, targetFileResource.getModFile(), "root.testsg.d1", "s1", 10, 20);
      assertNonAlignedMeasurementTimeRangeInTsFile(
          reader, targetFileResource.getModFile(), "root.testsg.d2", "s1", 21, 24);
    }
  }

  @Test
  public void testOneDeviceOneNonAlignedSeriesWithMultiOverlappedFiles1() throws IOException {
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
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    List<TsFileResource> targetFiles = tsFileManager.getTsFileList(true);
    Assert.assertFalse(targetFiles.get(0).getModFile().exists());
    Assert.assertTrue(targetFiles.get(1).getModFile().exists());
    Assert.assertTrue(targetFiles.get(2).getModFile().exists());
    assertDeviceTimeRangeInResource(targetFiles.get(0), "root.testsg.d1", 10, 20);
    assertDeviceTimeRangeInResource(targetFiles.get(1), "root.testsg.d1", 21, 50);
    assertDeviceTimeRangeInResource(targetFiles.get(2), "root.testsg.d1", 51, 100);
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(targetFiles.get(0).getTsFilePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(
          reader, targetFiles.get(0).getModFile(), "root.testsg.d1", "s1", 10, 20);
    }
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(targetFiles.get(1).getTsFilePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(
          reader, targetFiles.get(1).getModFile(), "root.testsg.d1", "s1", 21, 50);
    }
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(targetFiles.get(2).getTsFilePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(
          reader, targetFiles.get(2).getModFile(), "root.testsg.d1", "s1", 51, 100);
    }
  }

  @Test
  public void testOneDeviceOneNonAlignedSeriesWithMultiOverlappedFiles() throws IOException {
    List<TsFileResource> seqFiles = new ArrayList<>();
    List<TsFileResource> unseqFiles = new ArrayList<>();
    TsFileResource seqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(20, 30)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    TsFileResource seqResource2 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(60, 70)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource2.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);

    TsFileResource seqResource3 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(90, 100)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource3.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);

    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(10, 13)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);

    TsFileResource unseqResource2 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(40, 50)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            false);
    unseqResource2.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);

    TsFileResource unseqResource3 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(83, 89)},
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
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    List<TsFileResource> targetFiles = tsFileManager.getTsFileList(true);
    Assert.assertTrue(targetFiles.get(0).getModFile().exists());
    Assert.assertTrue(targetFiles.get(0).getModFile().exists());
    Assert.assertTrue(targetFiles.get(0).getModFile().exists());
    assertDeviceTimeRangeInResource(targetFiles.get(0), "root.testsg.d1", 10, 30);
    assertDeviceTimeRangeInResource(targetFiles.get(1), "root.testsg.d1", 40, 70);
    assertDeviceTimeRangeInResource(targetFiles.get(2), "root.testsg.d1", 83, 100);
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(targetFiles.get(0).getTsFilePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(
          reader, targetFiles.get(0).getModFile(), "root.testsg.d1", "s1", 10, 30);
    }
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(targetFiles.get(1).getTsFilePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(
          reader, targetFiles.get(1).getModFile(), "root.testsg.d1", "s1", 40, 70);
    }
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(targetFiles.get(2).getTsFilePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(
          reader, targetFiles.get(2).getModFile(), "root.testsg.d1", "s1", 83, 100);
    }
  }

  @Test
  public void testNonAlignedUnseqDeviceNotExistInSeqFiles() throws IOException {
    List<TsFileResource> seqFiles = new ArrayList<>();
    List<TsFileResource> unseqFiles = new ArrayList<>();
    TsFileResource seqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(20, 30)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    TsFileResource seqResource2 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(60, 70)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource2.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);

    TsFileResource seqResource3 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(90, 100)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResource3.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);

    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d2",
            "s1",
            new TimeRange[] {new TimeRange(10, 13)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);

    TsFileResource unseqResource2 =
        generateSingleNonAlignedSeriesFile(
            "d2",
            "s1",
            new TimeRange[] {new TimeRange(40, 50)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            false);
    unseqResource2.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);

    TsFileResource unseqResource3 =
        generateSingleNonAlignedSeriesFile(
            "d2",
            "s1",
            new TimeRange[] {new TimeRange(83, 89)},
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
            0, tsFileManager, seqFiles, unseqFiles, new InPlaceFastCompactionPerformer(), 100, 0);

    Assert.assertTrue(worker.processOneCompactionTask(t));
    validateCompactionResult(
        t.getSelectedSequenceFiles(),
        t.getSelectedUnsequenceFiles(),
        tsFileManager.getTsFileList(true));
    List<TsFileResource> targetFiles = tsFileManager.getTsFileList(true);
    Assert.assertFalse(targetFiles.get(0).getModFile().exists());
    Assert.assertFalse(targetFiles.get(1).getModFile().exists());
    Assert.assertFalse(targetFiles.get(2).getModFile().exists());
    assertDeviceTimeRangeInResource(targetFiles.get(0), "root.testsg.d1", 20, 30);
    assertDeviceTimeRangeInResource(targetFiles.get(1), "root.testsg.d1", 60, 70);
    assertDeviceTimeRangeInResource(targetFiles.get(2), "root.testsg.d1", 90, 100);
    assertDeviceTimeRangeInResource(targetFiles.get(2), "root.testsg.d2", 10, 89);
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(targetFiles.get(0).getTsFilePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(
          reader, targetFiles.get(0).getModFile(), "root.testsg.d1", "s1", 20, 30);
    }
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(targetFiles.get(1).getTsFilePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(
          reader, targetFiles.get(1).getModFile(), "root.testsg.d1", "s1", 60, 70);
    }
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(targetFiles.get(2).getTsFilePath())) {
      assertNonAlignedMeasurementTimeRangeInTsFile(
          reader, targetFiles.get(2).getModFile(), "root.testsg.d1", "s1", 90, 100);
      assertNonAlignedMeasurementTimeRangeInTsFile(
          reader, targetFiles.get(2).getModFile(), "root.testsg.d2", "s1", 10, 89);
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

  private TsFileResource generateSingleAlignedSeriesFile(
      String device,
      List<String> measurement,
      TimeRange[] chunkTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType,
      boolean isSeq)
      throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResource(isSeq);
    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    writer1.startChunkGroup(device);
    writer1.generateSimpleAlignedSeriesToCurrentDevice(
        measurement, chunkTimeRanges, encoding, compressionType);
    writer1.endChunkGroup();
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }

  private void validateCompactionResult(
      List<TsFileResource> sourceSeqFiles,
      List<TsFileResource> sourceUnSeqFiles,
      List<TsFileResource> targetFiles) {
    Assert.assertEquals(SystemInfo.getInstance().getCompactionFileNumCost().get(), 0);
    Assert.assertEquals(SystemInfo.getInstance().getCompactionMemoryCost().get(), 0L);
    for (TsFileResource resource : sourceSeqFiles) {
      String tsFilePath = resource.getTsFilePath();
      Assert.assertEquals(TsFileResourceStatus.DELETED, resource.getStatus());
      Assert.assertFalse(new File(tsFilePath + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(new File(tsFilePath + ModificationFile.FILE_SUFFIX).exists());
      Assert.assertFalse(new File(tsFilePath + ModificationFile.COMPACTION_FILE_SUFFIX).exists());
      Assert.assertFalse(
          new File(tsFilePath + IoTDBConstant.IN_PLACE_COMPACTION_TEMP_METADATA_FILE_SUFFIX)
              .exists());
      Assert.assertTrue(resource.tryWriteLock());
      resource.writeUnlock();
    }
    for (TsFileResource resource : sourceUnSeqFiles) {
      String tsFilePath = resource.getTsFilePath();
      Assert.assertEquals(TsFileResourceStatus.DELETED, resource.getStatus());
      Assert.assertFalse(new File(tsFilePath + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(new File(tsFilePath + ModificationFile.FILE_SUFFIX).exists());
      Assert.assertFalse(new File(tsFilePath + ModificationFile.COMPACTION_FILE_SUFFIX).exists());
      Assert.assertTrue(resource.tryWriteLock());
      resource.writeUnlock();
    }
    // validate target files
    for (TsFileResource resource : targetFiles) {
      String tsFilePath = resource.getTsFilePath();
      Assert.assertEquals(TsFileResourceStatus.NORMAL, resource.getStatus());
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(new File(tsFilePath + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertTrue(CompactionValidator.getInstance().validateSingleTsFile(resource));
      Assert.assertTrue(resource.tryWriteLock());
      resource.writeUnlock();
    }
    tsFileManager.writeLock("test");
    tsFileManager.writeUnlock();
  }

  private void assertNonAlignedMeasurementTimeRangeInTsFile(
      TsFileSequenceReader reader,
      ModificationFile modsFile,
      String deviceId,
      String measurementId,
      long startTime,
      long endTime)
      throws IOException {
    long pointNumOfDeviceInFile = 0;
    List<ChunkMetadata> chunkMetadataList =
        reader.getChunkMetadataList(new Path(deviceId, measurementId, true));
    long startTimeInMetadata = Long.MAX_VALUE, endTimeInMetadata = Long.MIN_VALUE;
    ModificationUtils.modifyChunkMetaData(
        chunkMetadataList, new ArrayList<>(modsFile.getModifications()));
    for (ChunkMetadata chunkMetadata : chunkMetadataList) {
      if (!chunkMetadata.isModified()) {
        startTimeInMetadata = Math.min(startTimeInMetadata, chunkMetadata.getStartTime());
        endTimeInMetadata = Math.max(endTimeInMetadata, chunkMetadata.getEndTime());
        pointNumOfDeviceInFile += chunkMetadata.getNumOfPoints();
        continue;
      }
      Chunk chunk = reader.readMemChunk(chunkMetadata);
      ChunkReader chunkReader = new ChunkReader(chunk, null);
      while (chunkReader.hasNextSatisfiedPage()) {
        BatchData batchData = chunkReader.nextPageData();
        while (batchData.hasCurrent()) {
          startTimeInMetadata = Math.min(startTimeInMetadata, batchData.currentTime());
          endTimeInMetadata = Math.max(endTimeInMetadata, batchData.currentTime());
          pointNumOfDeviceInFile++;
          batchData.next();
        }
      }
    }
    Assert.assertEquals(startTime, startTimeInMetadata);
    Assert.assertEquals(endTime, endTimeInMetadata);
  }

  private void assertAlignedMeasurementTimeRangeInTsFile(
      TsFileSequenceReader reader,
      ModificationFile modsFile,
      String deviceId,
      String measurementId,
      long startTime,
      long endTime)
      throws IOException, IllegalPathException {
    long pointNumOfDeviceInFile = 0;
    List<AlignedChunkMetadata> chunkMetadataList = reader.getAlignedChunkMetadata(deviceId);
    long startTimeInMetadata = Long.MAX_VALUE, endTimeInMetadata = Long.MIN_VALUE;
    List<List<Modification>> measurementModificationLists = new ArrayList<>();
    for (IChunkMetadata iChunkMetadata : chunkMetadataList.get(0).getValueChunkMetadataList()) {
      List<Modification> measurementMods = new ArrayList<>();
      for (Modification modification : modsFile.getModifications()) {
        if (modification.getPath().matchFullPath(new PartialPath(deviceId, iChunkMetadata.getMeasurementUid()))) {
          measurementMods.add(modification);
        }
      }
      measurementModificationLists.add(measurementMods);
    }
    ModificationUtils.modifyAlignedChunkMetaData(
        chunkMetadataList, measurementModificationLists);
    for (AlignedChunkMetadata chunkMetadata : chunkMetadataList) {
      if (!chunkMetadata.isModified()) {
        startTimeInMetadata = Math.min(startTimeInMetadata, chunkMetadata.getStartTime());
        endTimeInMetadata = Math.max(endTimeInMetadata, chunkMetadata.getEndTime());
        continue;
      }
        Chunk timeChunk = reader.readMemChunk((ChunkMetadata) chunkMetadata.getTimeChunkMetadata());
        List<Chunk> valueChunks = new ArrayList<>();
        for (IChunkMetadata valueChunkMetadata : chunkMetadata.getValueChunkMetadataList()) {
          valueChunks.add(reader.readMemChunk((ChunkMetadata) valueChunkMetadata));
        }

        AlignedChunkReader chunkReader = new AlignedChunkReader(timeChunk, valueChunks, null, true);
        while (chunkReader.hasNextSatisfiedPage()) {
          BatchData batchData = chunkReader.nextPageData();
          while (batchData.hasCurrent()) {
            startTimeInMetadata = Math.min(startTimeInMetadata, batchData.currentTime());
            endTimeInMetadata = Math.max(endTimeInMetadata, batchData.currentTime());
            pointNumOfDeviceInFile++;
            batchData.next();
          }
        }
      }
    Assert.assertEquals(startTime, startTimeInMetadata);
    Assert.assertEquals(endTime, endTimeInMetadata);
  }

  private void assertDeviceTimeRangeInResource(
      TsFileResource resource, String deviceId, long startTime, long endTime) {
    Assert.assertEquals(resource.getStartTime(deviceId), startTime);
    Assert.assertEquals(resource.getEndTime(deviceId), endTime);
  }
}
