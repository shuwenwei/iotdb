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

package org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CrossCompactionTaskResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileResource.class);

  private List<TsFileResource> seqFiles;
  private List<TsFileResource> unseqFiles;

  private long totalMemoryCost;
  private long totalFileSize;
  private float totalSeqFileSize;
  private float totalUnseqFileSize;
  private long totalFileNums;

  private boolean hasHardLinkSourceFile;
  private boolean isContainsLevelZeroFiles;

  private double overlapRatio;

  public CrossCompactionTaskResource() {
    this.seqFiles = new ArrayList<>();
    this.unseqFiles = new ArrayList<>();
    this.totalMemoryCost = 0L;
    this.totalFileSize = 0L;
    this.totalFileNums = 0L;
    this.isContainsLevelZeroFiles = false;
    this.overlapRatio = 1;
  }

  public List<TsFileResource> getSeqFiles() {
    return seqFiles;
  }

  // we need to unsure the files in seqFiles is ordered by the time range, that is, it should keep
  // the
  // order from candidates' seq file list.
  public void sortSeqFiles(List<TsFileResource> seqFilesCandidates) {
    Map<TsFileResource, Boolean> selectedFileMap = new HashMap<>();
    for (TsFileResource selectedFile : this.seqFiles) {
      selectedFileMap.put(selectedFile, true);
    }
    List<TsFileResource> sortedSeqFiles = new ArrayList<>();
    for (TsFileResource file : seqFilesCandidates) {
      if (selectedFileMap.containsKey(file)) {
        sortedSeqFiles.add(file);
      }
    }
    this.seqFiles = sortedSeqFiles;
  }

  public void putResources(
      TsFileResource unseqFile, List<TsFileResource> seqFiles, long memoryCost) {
    addUnseqFile(unseqFile);
    addTargetSeqFiles(seqFiles);
    updateMemoryCost(memoryCost);
  }

  private void addUnseqFile(TsFileResource file) {
    hasHardLinkSourceFile |= file.isHasHardLink();
    unseqFiles.add(file);
    totalUnseqFileSize += file.getTsFileSize();
    countStatistic(file);
  }

  private void addTargetSeqFiles(List<TsFileResource> targetSeqFiles) {
    targetSeqFiles.forEach(this::addSeqFile);
  }

  private void addSeqFile(TsFileResource file) {
    hasHardLinkSourceFile |= file.isHasHardLink();
    seqFiles.add(file);
    totalSeqFileSize += file.getTsFileSize();
    countStatistic(file);
    checkIsContainsLevelZeroFile(file);
  }

  private void checkIsContainsLevelZeroFile(TsFileResource file) {
    try {
      if (!isContainsLevelZeroFiles) {
        TsFileNameGenerator.TsFileName tsFileName =
            TsFileNameGenerator.getTsFileName(file.getTsFile().getName());
        if (tsFileName.getInnerCompactionCnt() == 0) {
          isContainsLevelZeroFiles = true;
        }
      }

    } catch (IOException e) {
      LOGGER.error("parse file name error : {}.", file.getTsFile().getName());
    }
  }

  private void updateMemoryCost(long newMemoryCost) {
    this.totalMemoryCost = Math.max(totalMemoryCost, newMemoryCost);
  }

  private void countStatistic(TsFileResource file) {
    totalFileSize += file.getTsFileSize();
    totalFileNums += 1;
  }

  public List<TsFileResource> getUnseqFiles() {
    return unseqFiles;
  }

  public long getTotalMemoryCost() {
    return totalMemoryCost;
  }

  public long getTotalFileSize() {
    return totalFileSize;
  }

  public float getTotalSeqFileSize() {
    return totalSeqFileSize;
  }

  public float getTotalUnseqFileSize() {
    return totalUnseqFileSize;
  }

  public long getTotalFileNums() {
    return totalFileNums;
  }

  public boolean isValid() {
    // Regarding current implementation of cross compaction task, the unseqFiles and seqFiles should
    // not be empty.
    // It should be changed once the task execution is optimized.
    // See https://issues.apache.org/jira/browse/IOTDB-5263
    return !unseqFiles.isEmpty() && !seqFiles.isEmpty();
  }

  public boolean isContainsLevelZeroFiles() {
    return isContainsLevelZeroFiles;
  }

  public boolean containsHardLinkSourceFile() {
    return hasHardLinkSourceFile;
  }

  public double getOverlapRatio() {
    return overlapRatio;
  }

  public void setOverlapRatio(double overlapRatio) {
    this.overlapRatio = overlapRatio;
  }
}
