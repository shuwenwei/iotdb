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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.inplace;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionFileCountExceededException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionMemoryNotEnoughException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionValidationFailedException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.InPlaceCompactionCleanupException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.InPlaceCompactionErrorException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.InPlaceFastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.validator.CompactionValidator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceList;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.storageengine.rescon.memory.TsFileResourceManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class InPlaceCrossSpaceCompactionTask extends AbstractCompactionTask {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private final List<InPlaceCompactionSeqFile> inPlaceCompactionSeqFiles;
  private final List<InPlaceCompactionUnSeqFile> inPlaceCompactionUnSeqFiles;
  private final List<TsFileResource> selectedSequenceFiles;
  private final List<TsFileResource> selectedUnsequenceFiles;
  private final List<TsFileResource> targetFiles;
  private final long memoryCost;
  private double selectedSeqFileSize;
  private double selectedUnseqFileSize;

  public InPlaceCrossSpaceCompactionTask(
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
    for (TsFileResource resource : selectedSequenceFiles) {
      this.selectedSeqFileSize += resource.getTsFileSize();
    }
    this.selectedUnsequenceFiles = selectedUnsequenceFiles;
    for (TsFileResource resource : selectedUnsequenceFiles) {
      this.selectedUnseqFileSize += resource.getTsFileSize();
    }
    // generate a copy of all source seq TsFileResource in memory
    this.targetFiles =
        selectedSequenceFiles.stream()
            .map(
                resource ->
                    new TsFileResource(resource.getTsFile(), TsFileResourceStatus.COMPACTING))
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
      inPlaceCompactionFile.analyzeFile();
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
    long startTime = System.currentTimeMillis();
    File logFile =
        new File(
            inPlaceCompactionSeqFiles.get(0).tsFileResource.getTsFile().getAbsolutePath()
                + CompactionLogger.IN_PLACE_CROSS_COMPACTION_LOG_NAME_SUFFIX);
    try (CompactionLogger logger = new CompactionLogger(logFile)) {

      initSeqFileInfo();
      recordLogBeforeDoingCompaction(logger);

      // 这个步骤如果发生了异常，需要进行文件恢复的内容包括：1. seqFile;
      splitAllSeqFilesIntoDataAndMeta();

      // 这个步骤如果发生了异常，需要进行文件恢复的内容包括：1. seqFile; 2. target resource, 3. mods ?
      performInPlaceCompaction();

      // 这个步骤如果发生了异常，需要进行文件恢复的内容包括：1. seqFile; 2. target resource, 3. 新产生的 mods、resource
      prepareAdjuvantFilesOfTargetResources();

      // 对目标文件进行重叠验证和文件正确性验证
      CompactionValidator validator = CompactionValidator.getInstance();
      if (!validator.validateCompaction(
          storageGroupName,
          tsFileManager,
          timePartition,
          selectedSequenceFiles,
          selectedUnsequenceFiles,
          targetFiles,
          false,
          true)) {
        throw new CompactionValidationFailedException("Failed to pass compaction validation");
      }

      atomicReplace();

      // >>> 删除已经用完的源文件及其附属文件，顺序文件已经被重命名，不需要执行删除
      removeRemainingSourceFiles();
      // <<< 删除已经用完的源文件及其附属文件
      CompactionMetrics.getInstance().recordSummaryInfo(summary);

      double costTime = (System.currentTimeMillis() - startTime) / 1000.0d;
      LOGGER.info(
          "{}-{} [Compaction] InPlaceCrossSpaceCompaction task finishes successfully, "
              + "time cost is {} s, "
              + "compaction speed is {} MB/s, {}",
          storageGroupName,
          dataRegionId,
          String.format("%.2f", costTime),
          String.format(
              "%.2f", (selectedSeqFileSize + selectedUnseqFileSize) / 1024.0d / 1024.0d / costTime),
          summary);
      return true;
    } catch (InPlaceCompactionErrorException | IOException e) {
      LOGGER.error("In place compaction error. Set AllCompaction to false.", e);
      tsFileManager.setAllowCompaction(false); // 考虑是否需要在这一步进行设定？
      LOGGER.info("start to execute failover for inplace compaction", e);
      revertCompactionAndRecoverToInitStatus();
      return false;
    } catch (InPlaceCompactionCleanupException e) {
      tsFileManager.setAllowCompaction(false); // 考虑是否需要在这一步进行设定？
      LOGGER.error("fetal error. InPlaceCompaction error when doing cleanup work", e);
      return false;
    } catch (CompactionValidationFailedException e) {
      LOGGER.error(
          "Failed to pass compaction validation, "
              + "source sequence files is: {}, "
              + "unsequence files is {}, "
              + "target files is {}",
          selectedSequenceFiles,
          selectedUnsequenceFiles,
          targetFiles);
      return false;
    } finally {
      // >>> 系统资源释放
      SystemInfo.getInstance().resetCompactionMemoryCost(memoryCost);
      SystemInfo.getInstance()
          .decreaseCompactionFileNumCost(
              inPlaceCompactionSeqFiles.size() + selectedUnsequenceFiles.size());
      // <<< 系统资源释放

      // >>> 重置原始文件
      List<InPlaceCompactionFile> files =
          new ArrayList<>(inPlaceCompactionSeqFiles.size() + inPlaceCompactionUnSeqFiles.size());
      files.addAll(inPlaceCompactionSeqFiles);
      files.addAll(inPlaceCompactionUnSeqFiles);
      for (InPlaceCompactionFile f : files) {
        try {
          f.releaseResourceAndResetStatus();
        } catch (IOException e) {
          LOGGER.error("Failed to release resource and reset status", e);
        }
      }
      // <<< 重置原始文件
      // 删除合并日志文件
      try {
        Files.deleteIfExists(logFile.toPath());
      } catch (IOException e) {
        LOGGER.error("Failed to delete compaction log file");
      }
    }
  }

  private void recordLogBeforeDoingCompaction(CompactionLogger logger)
      throws InPlaceCompactionErrorException {
    try {
      // record seq files in log
      for (InPlaceCompactionSeqFile f : this.inPlaceCompactionSeqFiles) {
        logger.logFile(
            f.tsFileResource,
            CompactionLogger.STR_SOURCE_FILES,
            f.getDataSize(),
            f.getMetadataSize());
      }
      // record unseq files in log
      for (InPlaceCompactionFile f : this.inPlaceCompactionUnSeqFiles) {
        logger.logFile(f.getTsFileResource(), CompactionLogger.STR_SOURCE_FILES);
      }
    } catch (IOException e) {
      throw new InPlaceCompactionErrorException("error when recording log before compaction", e);
    }
  }

  private void performInPlaceCompaction() throws InPlaceCompactionErrorException {
    try {
      performer.setSourceFiles(selectedSequenceFiles, selectedUnsequenceFiles);
      performer.setTargetFiles(targetFiles);
      performer.setSummary(summary);
      performer.perform();
    } catch (Exception e) {
      throw new InPlaceCompactionErrorException("error when performing in place compaction.", e);
    }
  }

  private void atomicReplace() throws InPlaceCompactionCleanupException {
    List<TsFileResource> removedSeqFiles = new ArrayList<>(selectedSequenceFiles.size());
    List<TsFileResource> removedUnSeqFiles = new ArrayList<>(selectedUnsequenceFiles.size());
    List<TsFileResource> addedTargetFiles = new ArrayList<>(targetFiles.size());
    Map<Path, Path> renamedFileMap = new HashMap<>(selectedSequenceFiles.size());

    TsFileResourceList seqListReference =
        tsFileManager.getOrCreateSequenceListByTimePartition(timePartition);
    TsFileResourceList unSeqListReference =
        tsFileManager.getOrCreateSequenceListByTimePartition(timePartition);
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
        Path targetFilePath =
            TsFileNameGenerator.getCrossSpaceCompactionTargetFile(resource, false).toPath();
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
      throw new InPlaceCompactionCleanupException("error when replace target files atomically", e);
    } finally {
      tsFileManager.writeUnlock();
    }
  }

  private void splitAllSeqFilesIntoDataAndMeta() throws InPlaceCompactionErrorException {
    for (InPlaceCompactionSeqFile f : this.inPlaceCompactionSeqFiles) {
      f.splitFileIntoDataAndMeta();
    }
  }

  private void prepareAdjuvantFilesOfTargetResources() throws InPlaceCompactionErrorException {
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
          ((InPlaceFastCompactionPerformer) performer).getRewriteDevices());
    } catch (Exception e) {
      throw new InPlaceCompactionErrorException(
          "Can not prepare resource and mods file for target TsFileResource", e);
    }
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

  public void revertCompactionAndRecoverToInitStatus() {
    try {
      revertSourceFiles();
      removeTargetResourceAndModsFiles();
    } catch (InPlaceCompactionErrorException e) {
      tsFileManager.setAllowCompaction(false);
      LOGGER.error(
          "fetal error. recover InPlaceCompactionTask failed. Please recover it manually", e);
    }
  }

  private void revertSourceFiles() throws InPlaceCompactionErrorException {
    for (InPlaceCompactionFile f : this.inPlaceCompactionSeqFiles) {
      f.revert();
    }
    for (InPlaceCompactionFile f : this.inPlaceCompactionUnSeqFiles) {
      f.revert();
    }
  }

  private void removeRemainingSourceFiles() throws InPlaceCompactionCleanupException {
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
      throw new InPlaceCompactionCleanupException("failed to remove remaining source files", e);
    }
  }

  private void removeTargetResourceAndModsFiles() throws InPlaceCompactionErrorException {
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
      throw new InPlaceCompactionErrorException(
          "Failed to remove target resource and mods files", e);
    }
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask otherTask) {
    if (!(otherTask instanceof InPlaceCrossSpaceCompactionTask)) {
      return false;
    }
    InPlaceCrossSpaceCompactionTask otherCrossCompactionTask =
        (InPlaceCrossSpaceCompactionTask) otherTask;
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
      inPlaceCompactionFile.releaseLock();
    }
    for (InPlaceCompactionFile inPlaceCompactionFile : inPlaceCompactionUnSeqFiles) {
      inPlaceCompactionFile.getTsFileResource().setStatus(TsFileResourceStatus.NORMAL);
      inPlaceCompactionFile.releaseLock();
    }
  }

  @Override
  protected void createSummary() {
    summary = new FastCompactionTaskSummary();
  }
}
