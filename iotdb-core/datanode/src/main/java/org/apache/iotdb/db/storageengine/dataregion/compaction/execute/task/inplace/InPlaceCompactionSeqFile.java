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
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.InPlaceCompactionErrorException;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public class InPlaceCompactionSeqFile extends InPlaceCompactionFile {
  private long dataSize;
  private long metadataSize;
  private FileChannel tsFileChannel;
  private FileChannel metaFileChannel;

  public InPlaceCompactionSeqFile(TsFileResource resource) {
    super(resource);
  }

  public void analyzeFile() throws IOException {
    File sourceFile = tsFileResource.getTsFile();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(sourceFile.getAbsolutePath())) {
      this.metadataSize = reader.getAllMetadataSize();
      this.dataSize = sourceFile.length() - metadataSize;
    }
  }

  public void splitFileIntoDataAndMeta() throws InPlaceCompactionErrorException {
    // 1. generate tmp meta file
    writeMetadataToMetaFile();
    // 2. release read lock and acquire write lock
    releaseAcquiredLockAndAcquireWriteLock();
    // 3. change TsFileResource status
    if (!updateTsFileResourceStatusToSplit()) {
      throw new InPlaceCompactionErrorException(
          String.format(
              "cannot set TsFileResource to SPLIT_DURING_COMPACTING. File: %s, current status: %s ",
              tsFileResource.getTsFilePath(), tsFileResource.getStatus()));
    }
    // 4. set dataSize to tsFileResource
    tsFileResource.setDataSize(this.dataSize);
    // 5. truncate source file
    truncateSourceFile();
    // 6. use special TsFileSequenceReader to occupy the placeholder in FileReaderManager
    try {
      FileReaderManager.getInstance()
          .occupyPlaceHolderWithCompactingTsFileReader(this.tsFileResource);
    } catch (IOException e) {
      throw new InPlaceCompactionErrorException(
          String.format(
              "cannot open file: %s, status: %s",
              tsFileResource.getTsFilePath(), tsFileResource.getStatus()));
    }
    // 7. release write lock and acquire read lock
    writeUnLock();
  }

  public void writeMetadataToMetaFile() throws InPlaceCompactionErrorException {
    try {
      Files.createFile(getMetadataFile().toPath());
      FileChannel src = getTsFileChannel();
      FileChannel dst = getMetaFileChannel();
      long transferSize = src.transferTo(dataSize, metadataSize, dst);
      if (transferSize != metadataSize) {
        throw new InPlaceCompactionErrorException(
            String.format(
                "error when writing metadata file. transferred size is not consistent. transferSize: %d, expected size: %d",
                transferSize, metadataSize));
      }
      dst.force(true);
    } catch (IOException e) {
      throw new InPlaceCompactionErrorException("error when writing tmp metadata file", e);
    }
  }

  public boolean updateTsFileResourceStatusToSplit() {
    return this.tsFileResource.setStatus(TsFileResourceStatus.SPLIT_DURING_COMPACTING);
  }

  public boolean updateTsFileResourceStatusToCompacting() {
    return this.tsFileResource.setStatus(TsFileResourceStatus.COMPACTING);
  }

  public void deleteMetadataFile() throws IOException {
    File metadataFile = getMetadataFile();
    if (metadataFile.exists()) {
      Files.delete(metadataFile.toPath());
    }
  }

  public void revert() throws InPlaceCompactionErrorException {
    releaseLock();
    try {
      writeLock();
      FileReaderManager.getInstance().releaseCompactingTsFileReaderPlaceholder(this.tsFileResource);
      if (needReWriteFileMetaToTail()) {
        // 重写尾部 meta

        FileChannel tsFileChannel = getTsFileChannel();
        FileChannel metaFileChannel = getMetaFileChannel();
        tsFileChannel.truncate(dataSize);
        tsFileChannel.position(dataSize);
        long transferSize = metaFileChannel.transferTo(0, metadataSize, tsFileChannel);
        if (transferSize != metadataSize) {
          throw new RuntimeException("Failed to recover TsFile");
        }
        tsFileChannel.force(true);
        updateTsFileResourceStatusToCompacting();
      }
      deleteMetadataFile();
      deleteCompactionModsFile();
      tsFileResource.setStatus(TsFileResourceStatus.NORMAL);
    } catch (IOException e) {
      throw new InPlaceCompactionErrorException("error when recover source file", e);
    } finally {
      writeUnLock();
    }
  }

  @Override
  public void releaseResourceAndResetStatus() throws IOException {
    // If the status of the TsFileResource is DELETED, this
    // method will fail
    tsFileResource.setStatus(TsFileResourceStatus.NORMAL);
    releaseLock();
    if (tsFileChannel != null) {
      tsFileChannel.close();
      tsFileChannel = null;
    }
    if (metaFileChannel != null) {
      metaFileChannel.close();
      metaFileChannel = null;
    }
  }

  public void truncateSourceFile() throws InPlaceCompactionErrorException {
    try {
      FileChannel tsFileChannel = getTsFileChannel();
      tsFileChannel.truncate(dataSize);
      tsFileChannel.force(true);
    } catch (IOException e) {
      throw new InPlaceCompactionErrorException(
          String.format("error when truncating source file: %s", tsFileResource.getTsFilePath()),
          e);
    }
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

  public File getMetadataFile() {
    File dataFile = this.tsFileResource.getTsFile();
    return new File(
        dataFile.getAbsolutePath() + IoTDBConstant.IN_PLACE_COMPACTION_TEMP_METADATA_FILE_SUFFIX);
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

  public void setDataSize(long dataSize) {
    this.dataSize = dataSize;
  }

  public long getMetadataSize() {
    return metadataSize;
  }

  public void setMetadataSize(long metadataSize) {
    this.metadataSize = metadataSize;
  }
}
