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
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class InplaceCrossSpaceCompactionTask extends AbstractCompactionTask {
  private List<InPlaceCompactionFile> inPlaceCompactionFiles;
  private List<TsFileResource> selectedUnsequenceFiles;
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
    this.inPlaceCompactionFiles = new ArrayList<>();
    selectedSequenceFiles.forEach(
        f -> this.inPlaceCompactionFiles.add(new InPlaceCompactionFile(f)));
    this.selectedUnsequenceFiles = selectedUnsequenceFiles;
    this.memoryCost = memoryCost;
  }

  private void init() {
    this.inPlaceCompactionFiles.forEach(InPlaceCompactionFile::prepare);
  }

  @Override
  protected List<TsFileResource> getAllSourceTsFiles() {
    return null;
  }

  @Override
  @SuppressWarnings({"squid:S6541", "squid:S3776", "squid:S2142"})
  public boolean doCompaction() {
    init();
    boolean isSuccess = true;
    long startTime = System.currentTimeMillis();

    File logFile =
        new File(
            inPlaceCompactionFiles.get(0).tsFile.getTsFile().getAbsolutePath()
                + CompactionLogger.CROSS_COMPACTION_LOG_NAME_SUFFIX);

    try (CompactionLogger logger = new CompactionLogger(logFile)) {
      for (InPlaceCompactionFile f : this.inPlaceCompactionFiles) {
        logger.logFile(f.tsFile, CompactionLogger.STR_SOURCE_FILES, f.dataSize, f.metadataSize);
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
        return false;
      }

      performer.setSourceFiles(selectedSequenceFiles, selectedUnsequenceFiles);
      performer.setTargetFiles(selectedSequenceFiles);
      performer.setSummary(summary);
      performer.perform();

      CompactionUtils.updateProgressIndex(
          selectedSequenceFiles, selectedSequenceFiles, selectedUnsequenceFiles);
      CompactionUtils.combineModsInInplaceCrossCompaction(
          selectedSequenceFiles,
          selectedUnsequenceFiles,
          dataSizeOfSourceSeqFiles,
          ((FastDeviceCompactionPerformer) performer).getRewriteDevices());

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
    return false;
  }

  private boolean prepareBeforePerform() {
    // 1. generate tmp meta file
    for (InPlaceCompactionFile f : this.inPlaceCompactionFiles) {
      if (!f.writeMetadataToMetaFile()) {
        return false;
      }
    }

    // 2. write lock
    for (InPlaceCompactionFile f : this.inPlaceCompactionFiles) {
      f.releaseReadLockAndWriteLock();
    }

    // 4. change status

    // 3. truncate

    // 5. read lock
    return true;
  }

  private boolean copyMetadataOfSeqFilesToTempFile(List<TsFileResource> seqResources) {
    try {
      for (TsFileResource resource : seqResources) {
        File srcTsFile = resource.getTsFile();
        File metadataFile = new File(srcTsFile.getAbsolutePath() + ".tail");
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
    FileChannel fileChannel =
        FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.APPEND);
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
        FileChannel metadataFileChannel =
            getFileChannel(new File(dataFile.getAbsolutePath() + ".meta"));
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

  private static class InPlaceCompactionFile {
    private TsFileResource tsFile;
    private long dataSize;
    private long metadataSize;
    private FileChannel tsFileChannel;
    private FileChannel metaFileChannel;
    private LockStatus lockStatus;

    private enum LockStatus {
      NO_LOCK,
      READ_LOCK,
      WRITE_LOCK
    }

    public InPlaceCompactionFile(TsFileResource tsFile) {
      this.tsFile = tsFile;
      // When one compaction task starts, the source file is always in READ_LOCK status
      this.lockStatus = LockStatus.READ_LOCK;
    }

    public void prepare() {}

    public boolean writeMetadataToMetaFile() {
      return false;
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
      tsFile.writeLock();
      lockStatus = LockStatus.WRITE_LOCK;
    }

    public void writeUnLock() {
      tsFile.writeUnlock();
      lockStatus = LockStatus.NO_LOCK;
    }

    public void readLock() {
      tsFile.readLock();
      lockStatus = LockStatus.READ_LOCK;
    }

    public void readUnLock() {
      tsFile.readUnlock();
      lockStatus = LockStatus.NO_LOCK;
    }

    public void recover() {
      if (needReWriteFileMetaToTail()) {
        // 重写尾部 meta
      } else {
        // 删除 meta 文件 if exist
      }
    }

    private boolean needReWriteFileMetaToTail() {
      if (tsFile.getStatus() == TsFileResourceStatus.SPLIT_DURING_COMPACTING) {
        return true;
      }
      long metaFileSize = 0L; // TODO: get real tmp meta file size
      return this.metadataSize == metaFileSize;
    }

    public void close() throws IOException {
      if (tsFileChannel != null) {
        tsFileChannel.close();
      }
      if (metaFileChannel != null) {
        metaFileChannel.close();
      }
    }
  }
}
