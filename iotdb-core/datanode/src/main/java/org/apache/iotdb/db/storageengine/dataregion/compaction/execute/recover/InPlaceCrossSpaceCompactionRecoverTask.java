/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.recover;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionRecoverException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.InPlaceCompactionErrorException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.inplace.InPlaceCompactionSeqFile;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.inplace.InPlaceCompactionUnSeqFile;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogAnalyzer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.TsFileIdentifier;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class InPlaceCrossSpaceCompactionRecoverTask {

  private final Logger logger = LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private final File compactionLogFile;
  private final String fullStorageGroupName;
  private final TsFileManager tsFileManager;

  public InPlaceCrossSpaceCompactionRecoverTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      TsFileManager tsFileManager,
      File logFile) {
    this.compactionLogFile = logFile;
    this.fullStorageGroupName = logicalStorageGroupName + "-" + virtualStorageGroupName;
    this.tsFileManager = tsFileManager;
  }

  public void doCompaction() {
    logger.info(
        "{} [Compaction][Recover] compaction log is {}", fullStorageGroupName, compactionLogFile);
    if (!compactionLogFile.exists()) {
      logger.error(
          "{} [Compaction][Recover] compaction log file {} not exists, abort recover",
          fullStorageGroupName,
          compactionLogFile);
      return;
    }
    logger.info(
        "{} [Compaction][Recover] compaction log file {} exists, start to recover it",
        fullStorageGroupName,
        compactionLogFile);
    CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(compactionLogFile);
    try {
      logAnalyzer.analyze();
    } catch (IOException e) {
      logger.error(
          "{} [Compaction][Recover] failed to analyze compaction log file {}, abort recover",
          fullStorageGroupName,
          compactionLogFile,
          e);
      return;
    }

    List<TsFileIdentifier> sourceFileIdentifiers = logAnalyzer.getSourceFileInfos();
    List<TsFileIdentifier> targetFileIdentifiers = logAnalyzer.getTargetFileInfos();
    List<TsFileIdentifier> deletedTargetFileIdentifiers = logAnalyzer.getDeletedTargetFileInfos();

    // compaction log file is incomplete
    if (targetFileIdentifiers.isEmpty() || sourceFileIdentifiers.isEmpty()) {
      logger.info(
          "{} [Compaction][Recover] incomplete log file, abort recover", fullStorageGroupName);
      return;
    }

    try {
      recoverCompactionFiles(
          sourceFileIdentifiers, targetFileIdentifiers, deletedTargetFileIdentifiers);
    } catch (CompactionRecoverException e) {
      logger.error(
          "{} [Compaction][Recover] failed to recover compaction log file {}, abort recover",
          fullStorageGroupName,
          compactionLogFile,
          e);
      tsFileManager.setAllowCompaction(false);
      return;
    }

    try {
      Files.deleteIfExists(compactionLogFile.toPath());
    } catch (IOException e) {
      logger.error(
          "{} [Compaction][Recover] failed to delete compaction log file {}",
          fullStorageGroupName,
          compactionLogFile,
          e);
    }
  }

  private void recoverCompactionFiles(
      List<TsFileIdentifier> sourceFileIdentifiers,
      List<TsFileIdentifier> targetFileIdentifiers,
      List<TsFileIdentifier> deletedTargetFileIdentifiers) {
    List<TsFileIdentifier> existSeqFiles = new ArrayList<>();
    List<TsFileIdentifier> existUnSeqFiles = new ArrayList<>();

    int sourceSeqFileNum = 0;
    for (TsFileIdentifier sourceFileIdentifier : sourceFileIdentifiers) {
      sourceSeqFileNum += sourceFileIdentifier.isSequence() ? 1 : 0;
      File sourceFile = sourceFileIdentifier.getFileFromDataDirsIfAnyAdjuvantFileExists();
      if (sourceFile == null) {
        continue;
      }
      if (!sourceFile.exists()) {
        continue;
      }
      if (sourceFileIdentifier.isSequence()) {
        existSeqFiles.add(sourceFileIdentifier);
      } else {
        existUnSeqFiles.add(sourceFileIdentifier);
      }
    }

    int existTargetFileNum = 0;
    for (TsFileIdentifier targetFileIdentifier : targetFileIdentifiers) {
      File targetFile = targetFileIdentifier.getFileFromDataDirs();
      if (targetFile != null && targetFile.exists()) {
        existTargetFileNum++;
      }
    }

    boolean allSourceFileExists =
        existSeqFiles.size() + existUnSeqFiles.size() == sourceFileIdentifiers.size();
    boolean canRecover =
        existSeqFiles.size() + existTargetFileNum + deletedTargetFileIdentifiers.size()
            >= sourceSeqFileNum;
    if (!canRecover) {
      logger.error(
          "{} [Compaction][Recover] Can not recover log file {} because some file is lost",
          fullStorageGroupName,
          compactionLogFile);
      return;
    }

    if (allSourceFileExists) {
      handleWithAllSourceFileExists(existSeqFiles, existUnSeqFiles, targetFileIdentifiers);
    } else {
      handleWithSomeSourceFileLost(
          existSeqFiles, existUnSeqFiles, sourceFileIdentifiers, deletedTargetFileIdentifiers);
    }
  }

  private void handleWithAllSourceFileExists(
      List<TsFileIdentifier> existSeqFiles,
      List<TsFileIdentifier> existUnSeqFiles,
      List<TsFileIdentifier> targetFileIdentifiers) {
    // recover source files
    for (TsFileIdentifier identifier : existSeqFiles) {
      TsFileResource resource = getTsFileResource(identifier);
      InPlaceCompactionSeqFile seqFile = null;
      try {
        seqFile = new InPlaceCompactionSeqFile(resource);
        seqFile.setDataSize(identifier.getDataSize());
        seqFile.setMetadataSize(identifier.getMetadataSize());
        seqFile.revert();
      } catch (InPlaceCompactionErrorException e) {
        throw new CompactionRecoverException();
      } finally {
        if (seqFile != null) {
          try {
            seqFile.releaseResourceAndResetStatus();
          } catch (IOException e) {
            logger.error(
                "{} [Compaction][Recover] Can not reset status of source file {}",
                fullStorageGroupName,
                seqFile,
                e);
          }
        }
      }
    }
    for (TsFileIdentifier identifier : existUnSeqFiles) {
      TsFileResource resource = getTsFileResource(identifier);
      InPlaceCompactionUnSeqFile unSeqFile = new InPlaceCompactionUnSeqFile(resource);
      try {
        unSeqFile.revert();
      } catch (InPlaceCompactionErrorException e) {
        throw new CompactionRecoverException(
            "Can not recover source unsequence file, " + unSeqFile);
      }
    }
    // remove target files
    for (TsFileIdentifier identifier : targetFileIdentifiers) {
      try {
        deleteResourceAndModsFile(identifier);
      } catch (IOException e) {
        logger.error(
            "{} [Compaction][Recover] can not delete target file {}",
            fullStorageGroupName,
            identifier.getFilePath(),
            e);
      }
    }
  }

  private void handleWithSomeSourceFileLost(
      List<TsFileIdentifier> existSeqFiles,
      List<TsFileIdentifier> existUnSeqFiles,
      List<TsFileIdentifier> sourceIdentifiers,
      List<TsFileIdentifier> deletedIdentifiers) {
    // 1. remove all source files
    // 2. rename seq files to target file
    try {

      for (TsFileIdentifier identifier : existSeqFiles) {
        TsFileResource resource = getTsFileResource(identifier);
        File targetFile = TsFileNameGenerator.getCrossSpaceCompactionTargetFile(resource, false);
        Files.move(resource.getTsFile().toPath(), targetFile.toPath());

        deleteResourceAndModsFile(resource);
      }
      for (TsFileIdentifier identifier : sourceIdentifiers) {
        if (identifier.isSequence()) {
          TsFileResource resource = getTsFileResource(identifier);
          if (resource != null) {
            deleteMetadataFileIfExists(resource);
          }
        }
        deleteResourceAndModsFile(identifier);
      }
      for (TsFileIdentifier identifier : deletedIdentifiers) {
        deleteResourceAndModsFile(identifier);
      }
    } catch (IOException e) {
      throw new CompactionRecoverException("Can not recover compaction files", e);
    }
  }

  private void deleteResourceAndModsFile(TsFileIdentifier identifier) throws IOException {
    TsFileResource resource = getTsFileResource(identifier);
    if (resource == null) {
      return;
    }
    deleteResourceAndModsFile(resource);
  }

  private void deleteResourceAndModsFile(TsFileResource resource) throws IOException {
    // delete compaction mods file
    resource.getCompactionModFile().remove();
    // delete resource and mods file
    resource.remove();
  }

  private void deleteMetadataFileIfExists(TsFileResource resource) throws IOException {
    String metadataFilePath =
        resource.getTsFile().getAbsolutePath()
            + IoTDBConstant.IN_PLACE_COMPACTION_TEMP_METADATA_FILE_SUFFIX;
    Files.deleteIfExists(new File(metadataFilePath).toPath());
  }

  private TsFileResource getTsFileResource(TsFileIdentifier identifier) {
    File f = identifier.getFileFromDataDirsIfAnyAdjuvantFileExists();
    if (f == null) {
      return null;
    }
    TsFileResource resource = new TsFileResource();
    resource.setFile(f);
    return resource;
  }
}
