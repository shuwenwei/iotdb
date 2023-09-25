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
import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionFileCountExceededException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionMemoryNotEnoughException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastDeviceCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceList;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.storageengine.rescon.memory.TsFileResourceManager;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    this.inPlaceCompactionSeqFiles =
        selectedSequenceFiles.stream()
            .map(InPlaceCompactionSeqFile::new)
            .collect(Collectors.toList());
    this.inPlaceCompactionUnSeqFiles =
        selectedUnsequenceFiles.stream()
            .map(InPlaceCompactionUnSeqFile::new)
            .collect(Collectors.toList());
    this.summary = new FastCompactionTaskSummary();
  }

  private void initSeqFileInfo() throws IOException {
    for (InPlaceCompactionSeqFile inPlaceCompactionFile : inPlaceCompactionSeqFiles) {
      inPlaceCompactionFile.prepare();
    }
  }

  @Override
  protected List<TsFileResource> getAllSourceTsFiles() {
    List<TsFileResource> allSourceTsFiles =
        new ArrayList<>(selectedSequenceFiles.size() + selectedUnsequenceFiles.size());
    allSourceTsFiles.addAll(selectedSequenceFiles);
    allSourceTsFiles.addAll(selectedUnsequenceFiles);
    return allSourceTsFiles;
  }

  @Override
  @SuppressWarnings({"squid:S6541", "squid:S3776", "squid:S2142"})
  public boolean doCompaction() {
    File logFile =
        new File(
            inPlaceCompactionSeqFiles.get(0).tsFileResource.getTsFile().getAbsolutePath()
                + CompactionLogger.IN_PLACE_CROSS_COMPACTION_LOG_NAME_SUFFIX);
    boolean isSuccess = true;
    try (CompactionLogger logger = new CompactionLogger(logFile)) {
      if(!prepareFileInfoAndTempFilesForPerform(logger)) {
        isSuccess = false;
        return false;
      }
      if (!performInPlaceCompaction()) {
        isSuccess = false;
        return false;
      }
      if (!prepareAdjuvantFilesOfTargetResources()) {
        isSuccess = false;
        return false;
      }
      // TODO modify current CompactionValidator to support InPlaceCompaction validation


      if (!atomicReplace()) {
        isSuccess = false;
        return false;
      }
      removeRemainingSourceFiles();
      CompactionMetrics.getInstance().recordSummaryInfo(summary);
    } catch (Exception e) {
      LOGGER.error("");
      tsFileManager.setAllowCompaction(false);
    } finally {
      if (!isSuccess) {
        recoverSeqFiles();
        removeTempFilesWhenCompactionFailed();
        removeTargetResourceAndModsFiles();
      }
      SystemInfo.getInstance().resetCompactionMemoryCost(memoryCost);
      SystemInfo.getInstance()
          .decreaseCompactionFileNumCost(
              inPlaceCompactionSeqFiles.size() + selectedUnsequenceFiles.size());

      releaseAllLocksAndResetStatus();
      for (InPlaceCompactionSeqFile seqFile : inPlaceCompactionSeqFiles) {
        try {
          seqFile.close();
        } catch (IOException e) {
          LOGGER.error("failed to close seq file {}", seqFile.getTsFileResource());
        }
      }
      for (TsFileResource unseqFile : selectedUnsequenceFiles) {
        try {
          unseqFile.close();
        } catch (IOException e) {
          LOGGER.error("failed to close unseq file {}", unseqFile);
        }
      }
    }
    return true;
  }

  private boolean prepareFileInfoAndTempFilesForPerform(CompactionLogger logger) {
    // prepare infos and temp files to perform compaction
    try {
      // get data size and metadata size of source seq files
      initSeqFileInfo();
      // record seq files in log
      for (InPlaceCompactionSeqFile f : this.inPlaceCompactionSeqFiles) {
        logger.logFile(
            f.tsFileResource, CompactionLogger.STR_SOURCE_FILES, f.dataSize, f.metadataSize);
      }
      // record unseq files in log
      for (InPlaceCompactionFile f : this.inPlaceCompactionUnSeqFiles) {
        logger.logFile(f.getTsFileResource(), CompactionLogger.STR_SOURCE_FILES);
      }
    } catch (IOException e) {
      LOGGER.error("Failed to prepare file info");
      return false;
    }
    // prepare files to perform InPlaceCompaction
    if (!prepareBeforePerform()) {
      recoverSeqFiles();
      return false;
    }
    return true;
  }

  private boolean performInPlaceCompaction() {
    try {
      performer.setSourceFiles(selectedSequenceFiles, selectedUnsequenceFiles);
      performer.setTargetFiles(targetFiles);
      performer.setSummary(summary);
      performer.perform();
    } catch (Exception e) {
      recoverSeqFiles();
      return false;
    }
    return true;
  }

  private boolean atomicReplace() {
    List<TsFileResource> removedSeqFiles = new ArrayList<>(selectedSequenceFiles.size());
    List<TsFileResource> removedUnSeqFiles = new ArrayList<>(selectedUnsequenceFiles.size());
    List<TsFileResource> addedTargetFiles = new ArrayList<>(targetFiles.size());
    Map<Path, Path> renamedFileMap = new HashMap<>(selectedSequenceFiles.size());

    TsFileResourceList seqListReference = tsFileManager.getOrCreateSequenceListByTimePartition(timePartition);
    TsFileResourceList unSeqListReference = tsFileManager.getOrCreateSequenceListByTimePartition(timePartition);
    tsFileManager.writeLock("InPlaceCompaction");
    try {
      for (TsFileResource resource : selectedSequenceFiles) {
        if (seqListReference.remove(resource)) {
          removedSeqFiles.add(resource);
          TsFileResourceManager.getInstance().removeTsFileResource(resource);
        }
      }
      for (TsFileResource resource : selectedUnsequenceFiles) {
        if (unSeqListReference.remove(resource)) {
          removedUnSeqFiles.add(resource);
          TsFileResourceManager.getInstance().removeTsFileResource(resource);
        }
      }
      for (TsFileResource resource : targetFiles) {
        if (!resource.isDeleted()) {
          seqListReference.add(resource);
          addedTargetFiles.add(resource);
          TsFileResourceManager.getInstance().registerSealedTsFileResource(resource);
        }
      }

      // acquire write lock of source seq files to wait all reading process finish
      releaseReadLockAndAcquireWriteLock(inPlaceCompactionSeqFiles);
      releaseReadLockAndAcquireWriteLock(inPlaceCompactionUnSeqFiles);

      // rename
      for (int i = 0; i < selectedSequenceFiles.size(); i++) {
        TsFileResource resource = selectedSequenceFiles.get(i);
        Path sourceFilePath = resource.getTsFile().toPath();
        Path targetFilePath = TsFileNameGenerator.getCrossSpaceCompactionTargetFile(resource, false).toPath();
        Files.move(sourceFilePath, targetFilePath);
        renamedFileMap.put(sourceFilePath, targetFilePath);
      }
      for (TsFileResource targetFile : targetFiles) {
        targetFile.setStatus(TsFileResourceStatus.NORMAL);
      }
    } catch (Exception e) {
      // undo replace
      try {
        for (TsFileResource resource : addedTargetFiles) {
          if (seqListReference.remove(resource)) {
            seqListReference.remove(resource);
            TsFileResourceManager.getInstance().removeTsFileResource(resource);
          }
        }
        for (TsFileResource resource : removedSeqFiles) {
          seqListReference.keepOrderInsert(resource);
          TsFileResourceManager.getInstance().registerSealedTsFileResource(resource);
        }
        for (TsFileResource resource : removedUnSeqFiles) {
          unSeqListReference.keepOrderInsert(resource);
          TsFileResourceManager.getInstance().registerSealedTsFileResource(resource);
        }
      } catch (IOException recoverException) {
        LOGGER.error("Failed to recover replace");
      }

      LOGGER.error("Failed to rename, recover");
      try {
        for (Map.Entry<Path, Path> entry : renamedFileMap.entrySet()) {
          Files.move(entry.getValue(), entry.getKey());
        }
      } catch (IOException recoverException) {
        LOGGER.error("Failed to recover rename");
      }
      return false;
    } finally {
      tsFileManager.writeUnlock();
    }
    return true;
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

  private boolean prepareAdjuvantFilesOfTargetResources() {
    try {
      CompactionUtils.updateProgressIndex(
          targetFiles, selectedSequenceFiles, selectedUnsequenceFiles);
      updateTargetTsFileResourceFiles(targetFiles);
      List<Long> dataSizeOfSourceSeqFiles =
          inPlaceCompactionSeqFiles.stream()
              .map(InPlaceCompactionSeqFile::getDataSize)
              .collect(Collectors.toList());
      CompactionUtils.combineModsInInPlaceCrossCompaction(
          selectedSequenceFiles,
          selectedUnsequenceFiles,
          dataSizeOfSourceSeqFiles,
          ((FastDeviceCompactionPerformer) performer).getRewriteDevices());
    } catch (Exception e) {
      LOGGER.error("Can not prepare resource and mods file for target TsFileResource");
      removeTargetResourceAndModsFiles();
      return false;
    }
    return true;
  }

  private void updateTargetTsFileResourceFiles(List<TsFileResource> targetFileResources)
      throws IOException {
    for (TsFileResource resource : targetFileResources) {
      File newTargetFile = TsFileNameGenerator.getCrossSpaceCompactionTargetFile(resource, false);
      resource.setFile(newTargetFile);
      resource.serialize();
      resource.closeWithoutSettingStatus();
    }
  }

  private void recoverSeqFiles() {
    for (InPlaceCompactionSeqFile f : this.inPlaceCompactionSeqFiles) {
      f.recover();
    }
  }

  private void removeRemainingSourceFiles() {
    try {
      for (TsFileResource sequenceResource : selectedSequenceFiles) {
        if (sequenceResource.getModFile().exists()) {
          FileMetrics.getInstance().decreaseModFileNum(1);
          FileMetrics.getInstance().decreaseModFileSize(sequenceResource.getModFile().getSize());
        }
      }

      for (TsFileResource unsequenceResource : selectedUnsequenceFiles) {
        if (unsequenceResource.getModFile().exists()) {
          FileMetrics.getInstance().decreaseModFileNum(1);
          FileMetrics.getInstance().decreaseModFileSize(unsequenceResource.getModFile().getSize());
        }
      }
      for (InPlaceCompactionSeqFile seqFile : inPlaceCompactionSeqFiles) {
        seqFile.deleteMetadataFile();
      }
      CompactionUtils.deleteSourceTsFileAndUpdateFileMetrics(
          selectedSequenceFiles, selectedUnsequenceFiles);
      CompactionUtils.deleteCompactionModsFile(selectedSequenceFiles, selectedUnsequenceFiles);
    } catch (Exception e) {
      LOGGER.error("Failed to remove remaining source files");
    }
  }

  private void removeTargetResourceAndModsFiles() {
    try {
      for (TsFileResource seqFile : selectedSequenceFiles) {
        File targetTsFile = TsFileNameGenerator.getCrossSpaceCompactionTargetFile(seqFile, false);
        String targetTsFilePath = targetTsFile.getAbsolutePath();
        File targetResourceFile = new File(targetTsFilePath + TsFileResource.RESOURCE_SUFFIX);
        File targetModsFile = new File(targetTsFilePath + TsFileResource.RESOURCE_SUFFIX);
        if (targetResourceFile.exists()) {
          Files.delete(targetResourceFile.toPath());
        }
        if (targetModsFile.exists()) {
          Files.delete(targetModsFile.toPath());
        }
      }
    } catch (IOException e) {
      LOGGER.error("Failed to remove target resource and mods files");
    }
  }

  private void removeTempFilesWhenCompactionFailed() {
    List<InPlaceCompactionFile> sourceFiles = new ArrayList<>(inPlaceCompactionSeqFiles.size() + inPlaceCompactionUnSeqFiles.size());
    for (InPlaceCompactionFile f : sourceFiles) {
      if (f.lockStatus == LockStatus.NO_LOCK) {
        f.writeLock();
      }
      if (f.lockStatus == LockStatus.READ_LOCK) {
        f.releaseReadLockAndWriteLock();
      }
      // delete metadata files
      if (f instanceof InPlaceCompactionSeqFile) {
        ((InPlaceCompactionSeqFile)f).deleteMetadataFile();
      }
    }
    // delete compaction mods files
    try {
      CompactionUtils.deleteCompactionModsFile(selectedSequenceFiles, selectedUnsequenceFiles);
    } catch (IOException e) {
      LOGGER.error("Failed to remove compaction mods files");
    }
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask otherTask) {
    if (!(otherTask instanceof InplaceCrossSpaceCompactionTask)) {
      return false;
    }
    InplaceCrossSpaceCompactionTask otherCrossCompactionTask =
        (InplaceCrossSpaceCompactionTask) otherTask;
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

  private void releaseReadLockAndAcquireWriteLock(List<? extends InPlaceCompactionFile> files) {
    for (InPlaceCompactionFile f : files) {
      f.releaseReadLockAndWriteLock();
    }
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

  private static abstract class InPlaceCompactionFile implements Closeable {
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

    @Override
    public void close() throws IOException {
      this.tsFileResource.close();
    }
  }

  private enum LockStatus {
    NO_LOCK,
    READ_LOCK,
    WRITE_LOCK
  }

  private static class InPlaceCompactionSeqFile extends InPlaceCompactionFile implements Closeable {
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
          tsFileChannel.position(dataSize);
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
    }

    public void deleteMetadataFile() {
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

    @Override
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
      return new File(dataFile.getAbsolutePath() + ".tail");
    }

    public FileChannel getTsFileChannel() throws IOException {
      if (tsFileChannel == null) {
        this.tsFileChannel =
            FileChannel.open(
                tsFileResource.getTsFile().toPath(),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
      }
      return this.tsFileChannel;
    }

    public FileChannel getMetaFileChannel() throws IOException {
      if (metaFileChannel == null) {
        File metaFile = getMetadataFile();
        this.metaFileChannel =
            FileChannel.open(metaFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
      }
      return this.metaFileChannel;
    }

    public long getDataSize() {
      return this.dataSize;
    }
  }
}
