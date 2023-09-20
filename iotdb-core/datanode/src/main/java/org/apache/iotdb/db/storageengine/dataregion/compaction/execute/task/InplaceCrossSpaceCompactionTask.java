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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionFileCountExceededException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionMemoryNotEnoughException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastDeviceCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class InplaceCrossSpaceCompactionTask extends AbstractCompactionTask {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private List<InPlaceCompactionSeqFile> inPlaceCompactionSeqFiles;
  private List<InPlaceCompactionUnSeqFile> inPlaceCompactionUnSeqFiles;
  private List<TsFileResource> selectedSequenceFiles;
  private List<TsFileResource> selectedUnsequenceFiles;
  private List<TsFileResource> targetFiles;
  private long memoryCost;

  public InplaceCrossSpaceCompactionTask(
      long timePartition,
      TsFileManager tsFileManager,
      List<TsFileResource> selectedSequenceFiles,
      List<TsFileResource> selectedUnsequenceFiles,
      ICrossCompactionPerformer performer,
      AtomicInteger currentTaskNum,
      long memoryCost,
      long serialId) {
    super(
        tsFileManager.getStorageGroupName(),
        tsFileManager.getDataRegionId(),
        timePartition,
        tsFileManager,
        currentTaskNum,
        serialId);
    this.selectedSequenceFiles = selectedSequenceFiles;
    this.selectedUnsequenceFiles = selectedUnsequenceFiles;
    // generate a copy of all source seq TsFileResource in memory
    this.targetFiles = selectedSequenceFiles
        .stream()
        .map(resource -> new TsFileResource(resource.getTsFile(), TsFileResourceStatus.COMPACTING))
        .collect(Collectors.toList());
    this.memoryCost = memoryCost;
    this.performer = performer;
    this.inPlaceCompactionSeqFiles = selectedSequenceFiles
        .stream()
        .map(InPlaceCompactionSeqFile::new)
        .collect(Collectors.toList());
    this.inPlaceCompactionUnSeqFiles = selectedUnsequenceFiles
        .stream()
        .map(InPlaceCompactionUnSeqFile::new)
        .collect(Collectors.toList());
  }

  private void init() throws IOException {
    for (InPlaceCompactionSeqFile inPlaceCompactionFile : inPlaceCompactionSeqFiles) {
      inPlaceCompactionFile.prepare();
    }
  }

  @Override
  protected List<TsFileResource> getAllSourceTsFiles() {
    List<TsFileResource> allSourceTsFiles = new ArrayList<>(selectedSequenceFiles.size() + selectedUnsequenceFiles.size());
    allSourceTsFiles.addAll(selectedSequenceFiles);
    allSourceTsFiles.addAll(selectedUnsequenceFiles);
    return allSourceTsFiles;
  }

  @Override
  @SuppressWarnings({"squid:S6541", "squid:S3776", "squid:S2142"})
  public boolean doCompaction() {
    boolean isSuccess = true;
    long startTime = System.currentTimeMillis();

    File logFile =
        new File(
            inPlaceCompactionSeqFiles.get(0).tsFileResource.getTsFile().getAbsolutePath()
                + CompactionLogger.IN_PLACE_CROSS_COMPACTION_LOG_NAME_SUFFIX);

    try (CompactionLogger logger = new CompactionLogger(logFile)) {
      init();
      for (InPlaceCompactionSeqFile f : this.inPlaceCompactionSeqFiles) {
        logger.logFile(f.tsFileResource, CompactionLogger.STR_SOURCE_FILES, f.dataSize, f.metadataSize);
      }
      for (TsFileResource f : this.selectedUnsequenceFiles) {
        logger.logFile(f, CompactionLogger.STR_SOURCE_FILES);
      }

      // prepare stage
      // 1. copy metadata part of each file as a tmp file named xx.tsfile.meta
      // 2. set status ? (maybe unnecessary)
      // 3. truncate original file at the start position of metadata
      // 4. move write offset to the end of file
      if (!prepareBeforePerform()) {
        // TODO
        throw new RuntimeException();
      }

      performer.setSourceFiles(selectedSequenceFiles, selectedUnsequenceFiles);
      performer.setTargetFiles(targetFiles);
      performer.setSummary(summary);
      performer.perform();


      replaceFileReferenceInTsFileResources(targetFiles);
      CompactionUtils.updateProgressIndex(
          targetFiles, selectedSequenceFiles, selectedUnsequenceFiles);
      List<Long> dataSizeOfSourceSeqFiles =
          inPlaceCompactionSeqFiles
              .stream()
              .map(InPlaceCompactionSeqFile::getDataSize)
              .collect(Collectors.toList());
      CompactionUtils.combineModsInInPlaceCrossCompaction(
          selectedSequenceFiles,
          selectedUnsequenceFiles,
          dataSizeOfSourceSeqFiles,
          ((FastDeviceCompactionPerformer) performer).getRewriteDevices());


    } catch (Exception e) {
      isSuccess = false;
      recoverSeqFiles();
    } finally {
      SystemInfo.getInstance().resetCompactionMemoryCost(memoryCost);
      SystemInfo.getInstance()
          .decreaseCompactionFileNumCost(
              inPlaceCompactionSeqFiles.size() + selectedUnsequenceFiles.size());
      releaseAllLocksAndResetStatus();
    }
    return isSuccess;
  }

  private boolean prepareBeforePerform() {
    for (InPlaceCompactionSeqFile f : this.inPlaceCompactionSeqFiles) {
      // 1. generate tmp meta file
      if (!f.writeMetadataToMetaFile()) {
        return false;
      }
      // 2. release read lock and acquire write lock
      f.releaseReadLockAndWriteLock();

      // 3. change TsFileResource status
      if (!f.updateTsFileResourceStatusToSplit()) {
        return false;
      }

      // 4. truncate source file
      if (!f.truncateSourceFile()) {
        return false;
      }
      // 5. release write lock and acquire read lock
      f.releaseWriteLockAndReadLock();
    }
    return true;
  }

  private void replaceFileReferenceInTsFileResources(List<TsFileResource> targetFileResources) throws IOException {
    for (TsFileResource resource : targetFileResources) {
      File newTargetFile = TsFileNameGenerator.getCrossSpaceCompactionTargetFile(resource, false);
      resource.setFile(newTargetFile);
    }
  }

  private void recoverSeqFiles() {
    for (InPlaceCompactionSeqFile f : this.inPlaceCompactionSeqFiles) {
      f.recover();
    }
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask otherTask) {
    if (!(otherTask instanceof InplaceCrossSpaceCompactionTask)) {
      return false;
    }
    InplaceCrossSpaceCompactionTask otherCrossCompactionTask = (InplaceCrossSpaceCompactionTask) otherTask;
    return this.selectedSequenceFiles.equals(otherCrossCompactionTask.selectedSequenceFiles)
        && this.selectedUnsequenceFiles.equals(otherCrossCompactionTask.selectedUnsequenceFiles)
        && this.performer.getClass().isInstance(otherCrossCompactionTask.performer);
  }

  @Override
  public boolean checkValidAndSetMerging() {
    if (!tsFileManager.isAllowCompaction()) {
      resetCompactionCandidateStatusForAllSourceFiles();
      return false;
    }
    try {
      SystemInfo.getInstance().addCompactionMemoryCost(memoryCost, 60);
      SystemInfo.getInstance()
          .addCompactionFileNum(selectedSequenceFiles.size() + selectedUnsequenceFiles.size(), 60);
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        LOGGER.warn("Interrupted when allocating memory for compaction", e);
        Thread.currentThread().interrupt();
      } else if (e instanceof CompactionMemoryNotEnoughException) {
        LOGGER.info("No enough memory for current compaction task {}", this, e);
      } else if (e instanceof CompactionFileCountExceededException) {
        LOGGER.info("No enough file num for current compaction task {}", this, e);
        SystemInfo.getInstance().resetCompactionMemoryCost(memoryCost);
      }
      resetCompactionCandidateStatusForAllSourceFiles();
      return false;
    }

    boolean addReadLockSuccess =
        addReadLockAndSetStatusCompacting(inPlaceCompactionSeqFiles)
            && addReadLockAndSetStatusCompacting(inPlaceCompactionUnSeqFiles);
    if (!addReadLockSuccess) {
      SystemInfo.getInstance().resetCompactionMemoryCost(memoryCost);
      SystemInfo.getInstance()
          .decreaseCompactionFileNumCost(
              selectedSequenceFiles.size() + selectedUnsequenceFiles.size());
    }
    return addReadLockSuccess;
  }

  private boolean addReadLockAndSetStatusCompacting(List<? extends InPlaceCompactionFile> files) {
    try {
      for (InPlaceCompactionFile f : files) {
        f.readLock();
        if (!f.getTsFileResource().setStatus(TsFileResourceStatus.COMPACTING)) {
          releaseAllLocksAndResetStatus();
          return false;
        }
      }
    } catch (Exception e) {
      releaseAllLocksAndResetStatus();
      throw e;
    }
    return true;
  }

  private void releaseAllLocksAndResetStatus() {
    for (InPlaceCompactionFile inPlaceCompactionFile : inPlaceCompactionSeqFiles) {
      inPlaceCompactionFile.getTsFileResource().setStatus(TsFileResourceStatus.NORMAL);
      inPlaceCompactionFile.release();
    }
    for (InPlaceCompactionFile inPlaceCompactionFile : inPlaceCompactionUnSeqFiles) {
      inPlaceCompactionFile.getTsFileResource().setStatus(TsFileResourceStatus.NORMAL);
      inPlaceCompactionFile.release();
    }
  }

  @Override
  protected void createSummary() {
    summary = new FastCompactionTaskSummary();
  }

  private static class InPlaceCompactionFile {
    protected TsFileResource tsFileResource;
    protected LockStatus lockStatus;

    public InPlaceCompactionFile(TsFileResource resource) {
      this.tsFileResource = resource;
      this.lockStatus = LockStatus.NO_LOCK;
    }

    public TsFileResource getTsFileResource() {
      return tsFileResource;
    }

    // release held write/read lock and do some other cleanup things.
    public void release() {
      if (lockStatus == LockStatus.READ_LOCK) {
        readUnLock();
      }
      if (lockStatus == LockStatus.WRITE_LOCK) {
        writeUnLock();
      }
    }

    public void releaseReadLockAndWriteLock() {
      readUnLock();
      writeLock();
    }

    public void releaseWriteLockAndReadLock() {
      writeUnLock();
      readLock();
    }

    public void writeLock() {
      tsFileResource.writeLock();
      lockStatus = LockStatus.WRITE_LOCK;
    }

    public void writeUnLock() {
      tsFileResource.writeUnlock();
      lockStatus = LockStatus.NO_LOCK;
    }

    public void readLock() {
      tsFileResource.readLock();
      lockStatus = LockStatus.READ_LOCK;
    }

    public void readUnLock() {
      tsFileResource.readUnlock();
      lockStatus = LockStatus.NO_LOCK;
    }

  }

  private static class InPlaceCompactionUnSeqFile extends InPlaceCompactionFile {

    public InPlaceCompactionUnSeqFile(TsFileResource resource) {
      super(resource);
    }

  }

  private enum LockStatus {
    NO_LOCK,
    READ_LOCK,
    WRITE_LOCK
  }

  private static class InPlaceCompactionSeqFile extends InPlaceCompactionFile {
    private long dataSize;
    private long metadataSize;
    private FileChannel tsFileChannel;
    private FileChannel metaFileChannel;

    public InPlaceCompactionSeqFile(TsFileResource resource) {
      super(resource);
    }

    public void prepare() throws IOException {
      File sourceFile = tsFileResource.getTsFile();
      try (TsFileSequenceReader reader = new TsFileSequenceReader(sourceFile.getAbsolutePath())) {
        this.metadataSize = reader.getAllMetadataSize();
        this.dataSize = sourceFile.length() - metadataSize;
      }
    }

    public boolean writeMetadataToMetaFile() {
      try {
        Files.createFile(getMetadataFile().toPath());
        FileChannel src = getTsFileChannel();
        FileChannel dst = getMetaFileChannel();
        long transferSize = src.transferTo(dataSize, metadataSize, dst);
        if (transferSize != metadataSize) {
          return false;
        }
        dst.force(true);
      } catch (IOException e) {

        return false;
      }
      return true;
    }

    public boolean updateTsFileResourceStatusToSplit() {
      return this.tsFileResource.setStatus(TsFileResourceStatus.SPLIT_DURING_COMPACTING);
    }

    public boolean updateTsFileResourceStatusToCompacting() {
      return this.tsFileResource.setStatus(TsFileResourceStatus.COMPACTING);
    }

    public void recover() {
      if (needReWriteFileMetaToTail()) {
        // 重写尾部 meta
        try {
          FileChannel tsFileChannel = getTsFileChannel();
          FileChannel metaFileChannel = getMetaFileChannel();
          tsFileChannel.truncate(dataSize);
          long transferSize = metaFileChannel.transferTo(0, metadataSize, tsFileChannel);
          if (transferSize != metadataSize) {
            throw new RuntimeException("Failed to recover TsFile");
          }
          tsFileChannel.force(true);

          if (TsFileResourceStatus.SPLIT_DURING_COMPACTING.equals(tsFileResource.getStatus())) {
            release();
            writeLock();
            updateTsFileResourceStatusToCompacting();
            releaseWriteLockAndReadLock();
          }
        } catch (Exception e) {
          // TODO
        }
      }
      // 删除 meta 文件 if exist
      File metadataFile = getMetadataFile();
      if (metadataFile.exists()) {
        try {
          Files.delete(metadataFile.toPath());
        } catch (IOException e) {
          // TODO
          LOGGER.error("Failed to delete meta file {}", metadataFile.getAbsoluteFile());
        }
      }
    }

    public boolean truncateSourceFile() {
      try {
        FileChannel tsFileChannel = getTsFileChannel();
        tsFileChannel.truncate(dataSize);
        tsFileChannel.force(true);
      } catch (IOException e) {
        return false;
      }
      return true;
    }

    private boolean needReWriteFileMetaToTail() {
      if (tsFileResource.getStatus() == TsFileResourceStatus.SPLIT_DURING_COMPACTING) {
        return true;
      }
      // TODO: get real tmp meta file size
      File metadataFile = getMetadataFile();
      if (!metadataFile.exists()) {
        return false;
      }
      return this.metadataSize == metadataFile.length();
    }

    public void close() throws IOException {
      if (tsFileChannel != null) {
        tsFileChannel.close();
        tsFileChannel = null;
      }
      if (metaFileChannel != null) {
        metaFileChannel.close();
        metaFileChannel = null;
      }
    }

    public File getMetadataFile() {
      File dataFile = this.tsFileResource.getTsFile();
      return new File(dataFile.getAbsolutePath() + ".meta");
    }

    public FileChannel getTsFileChannel() throws IOException {
      if (tsFileChannel == null) {
        this.tsFileChannel = FileChannel.open(tsFileResource.getTsFile().toPath(), StandardOpenOption.READ, StandardOpenOption.APPEND);
      }
      return this.tsFileChannel;
    }

    public FileChannel getMetaFileChannel() throws IOException {
      if (metaFileChannel == null) {
        File metaFile = getMetadataFile();
        this.metaFileChannel = FileChannel.open(metaFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
      }
      return this.metaFileChannel;
    }

    public long getDataSize() {
      return this.dataSize;
    }
  }
}
