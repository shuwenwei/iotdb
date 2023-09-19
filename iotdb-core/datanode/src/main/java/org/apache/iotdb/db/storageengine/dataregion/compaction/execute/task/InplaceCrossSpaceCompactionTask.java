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

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastDeviceCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class InplaceCrossSpaceCompactionTask extends CrossSpaceCompactionTask {

  private List<Long> dataSizeOfSourceSeqFiles;
  private Map<File, FileChannel> fileChannelCache;

  public InplaceCrossSpaceCompactionTask(long timePartition, TsFileManager tsFileManager, List<TsFileResource> selectedSequenceFiles, List<TsFileResource> selectedUnsequenceFiles, ICrossCompactionPerformer performer, AtomicInteger currentTaskNum, long memoryCost, long serialId) {
    super(timePartition, tsFileManager, selectedSequenceFiles, selectedUnsequenceFiles, performer, currentTaskNum, memoryCost, serialId);
    this.dataSizeOfSourceSeqFiles = new ArrayList<>(selectedSequenceFiles.size());
    this.fileChannelCache = new HashMap<>(selectedSequenceFiles.size() * 2);
  }

  @Override
  protected List<TsFileResource> getAllSourceTsFiles() {
    return super.getAllSourceTsFiles();
  }

  @Override
  @SuppressWarnings({"squid:S6541", "squid:S3776", "squid:S2142"})
  public boolean doCompaction() {
    boolean isSuccess = true;
    if (!tsFileManager.isAllowCompaction()) {
      return true;
    }
    long startTime = System.currentTimeMillis();
    targetTsfileResourceList = new ArrayList<>(selectedSequenceFiles);

    if (selectedSequenceFiles.isEmpty()
        || selectedUnsequenceFiles.isEmpty()) {
      LOGGER.info(
          "{}-{} [Compaction] Cross space compaction file list is empty, end it",
          storageGroupName,
          dataRegionId);
      return true;
    }

    for (TsFileResource resource : selectedSequenceFiles) {
      selectedSeqFileSize += resource.getTsFileSize();
    }

    for (TsFileResource resource : selectedUnsequenceFiles) {
      selectedUnseqFileSize += resource.getTsFileSize();
    }

    LOGGER.info(
        "{}-{} [Compaction] CrossSpaceCompaction task starts with {} seq files "
            + "and {} unsequence files. "
            + "Sequence files : {}, unsequence files : {} . "
            + "Sequence files size is {} MB, "
            + "unsequence file size is {} MB, "
            + "total size is {} MB",
        storageGroupName,
        dataRegionId,
        selectedSequenceFiles.size(),
        selectedUnsequenceFiles.size(),
        selectedSequenceFiles,
        selectedUnsequenceFiles,
        selectedSeqFileSize / 1024 / 1024,
        selectedUnseqFileSize / 1024 / 1024,
        (selectedSeqFileSize + selectedUnseqFileSize) / 1024 / 1024);

    logFile =
        new File(
            selectedSequenceFiles.get(0).getTsFile().getParent()
                + File.separator
                + targetTsfileResourceList.get(0).getTsFile().getName()
                + CompactionLogger.CROSS_COMPACTION_LOG_NAME_SUFFIX);

    try (CompactionLogger logger = new CompactionLogger(logFile)) {
      if (copyMetadataOfSeqFilesToTempFile(selectedSequenceFiles)) {
        return false;
      }
      releaseReadAndLockWrite(selectedSequenceFiles);

      if (!truncateSeqFilesAndUpdateStatus(selectedSequenceFiles)) {
        recoverAllSeqFile(selectedSequenceFiles);
        return false;
      }
      releaseWriteAndLockRead(selectedSequenceFiles);

      performer.setSourceFiles(selectedSequenceFiles, selectedUnsequenceFiles);
      performer.setTargetFiles(selectedSequenceFiles);
      performer.setSummary(summary);
      performer.perform();

      CompactionUtils.updateProgressIndex(selectedSequenceFiles, selectedSequenceFiles, selectedUnsequenceFiles);
      CompactionUtils.combineModsInInplaceCrossCompaction(selectedSequenceFiles, selectedUnsequenceFiles, dataSizeOfSourceSeqFiles, ((FastDeviceCompactionPerformer) performer).getRewriteDevices());



    } catch (IOException e) {
      for (Map.Entry<File, FileChannel> entry : fileChannelCache.entrySet()) {
        try {
          entry.getValue().close();
        } catch (IOException ioException) {
          ioException.printStackTrace();
        }
      }
      return false;
    } catch (Exception e) {
      e.printStackTrace();
    } finally {

    }

  }

  private boolean copyMetadataOfSeqFilesToTempFile(List<TsFileResource> seqResources) {
    try {
      for (TsFileResource resource : seqResources) {
        File srcTsFile = resource.getTsFile();
        File metadataFile = new File(srcTsFile.getAbsolutePath() + ".mt");
        Files.createFile(metadataFile.toPath());

        long fileSize = srcTsFile.length();
        long metadataSize = getAllMetadataSize(resource);
        this.dataSizeOfSourceSeqFiles.add(fileSize - metadataSize);

        if (!copyMetadataToTempFile(srcTsFile, metadataFile, metadataSize)) {
          throw new RuntimeException("Failed to copy metadata to temp file");
        }
      }
    } catch (Exception e) {
      deleteAllMetadataTempFile(seqResources);
      return false;
    }
    return true;
  }

  private long getAllMetadataSize(TsFileResource resource) throws IOException {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(resource.getTsFilePath())) {
      return reader.getAllMetadataSize();
    }
  }

  private boolean copyMetadataToTempFile(File srcFile, File metadataFile, long metadataSize) {
    try {
      FileChannel src = fileChannelCache.get(srcFile);
      FileChannel dst = fileChannelCache.get(metadataFile);
      long fileSize = srcFile.length();
      long transferSize = src.transferTo(fileSize - metadataSize, metadataSize, dst);
      if (transferSize != metadataSize) {
        return false;
      }
      dst.force(true);
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  private FileChannel getFileChannel(File file) throws IOException {
    if (fileChannelCache.containsKey(file)) {
      return fileChannelCache.get(file);
    }
    FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.APPEND);
    fileChannelCache.put(file, fileChannel);
    return fileChannel;
  }

  private boolean truncateSeqFilesAndUpdateStatus(List<TsFileResource> seqResources) {
    for (int i = 0; i < seqResources.size(); i++) {
      TsFileResource resource = seqResources.get(i);
      long dataSize = dataSizeOfSourceSeqFiles.get(i);
      try {
        FileChannel channel = getFileChannel(resource.getTsFile());
        channel.truncate(dataSize);
        channel.force(true);

        // resource.setStatus();
      } catch (IOException e) {
        return false;
      }
    }
    return true;
  }

  private void deleteAllMetadataTempFile(List<TsFileResource> seqResources) {
    try {
      for (TsFileResource resource : seqResources) {
        File metadataFile = new File(resource.getTsFile().getAbsolutePath() + ".mt");
        if (metadataFile.exists()) {
          Files.delete(metadataFile.toPath());
        }
      }
    } catch (IOException e) {
      // todo

    }
  }

  private void recoverAllSeqFile(List<TsFileResource> seqResources) {
    try {
      for (int i = 0; i < seqResources.size(); i++) {
        TsFileResource resource = seqResources.get(i);
        File dataFile = resource.getTsFile();
        FileChannel dataFileChannel = getFileChannel(dataFile);
        if (dataFile.length() != dataSizeOfSourceSeqFiles.get(i)) {
          dataFileChannel.truncate(dataSizeOfSourceSeqFiles.get(i));
        }
        FileChannel metadataFileChannel = getFileChannel(new File(dataFile.getAbsolutePath() + ".meta"));
        metadataFileChannel.transferTo(0, metadataFileChannel.size(), dataFileChannel);
        dataFileChannel.force(true);
      }
    } catch (IOException e) {

    }
  }

  protected void releaseWriteAndLockRead(List<TsFileResource> tsFileResourceList) {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      tsFileResource.writeUnlock();
      holdWriteLockList.remove(tsFileResource);
      tsFileResource.readLock();
      holdReadLockList.add(tsFileResource);
    }
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask otherTask) {
    return super.equalsOtherTask(otherTask);
  }

  @Override
  public boolean checkValidAndSetMerging() {
    return super.checkValidAndSetMerging();
  }

  @Override
  protected void createSummary() {
    summary = new FastCompactionTaskSummary();
  }
}
