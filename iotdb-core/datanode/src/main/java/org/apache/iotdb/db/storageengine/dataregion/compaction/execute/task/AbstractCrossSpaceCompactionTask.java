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
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({"squid:S1206", "squid:S2160"})
public abstract class AbstractCrossSpaceCompactionTask extends AbstractCompactionTask {

  protected List<TsFileResource> selectedSequenceFiles;
  protected List<TsFileResource> selectedUnsequenceFiles;
  protected double selectedSeqFileSize = 0;
  protected double selectedUnseqFileSize = 0;

  protected AbstractCrossSpaceCompactionTask(
      long timePartition,
      TsFileManager tsFileManager,
      List<TsFileResource> selectedSequenceFiles,
      List<TsFileResource> selectedUnsequenceFiles,
      ICrossCompactionPerformer performer,
      long memoryCost,
      long serialId) {
    super(
        tsFileManager.getStorageGroupName(),
        tsFileManager.getDataRegionId(),
        timePartition,
        tsFileManager,
        serialId);
    this.selectedSequenceFiles = selectedSequenceFiles;
    this.selectedUnsequenceFiles = selectedUnsequenceFiles;
    for (TsFileResource resource : selectedSequenceFiles) {
      selectedSeqFileSize += resource.getTsFileSize();
    }
    for (TsFileResource resource : selectedUnsequenceFiles) {
      selectedUnseqFileSize += resource.getTsFileSize();
    }
    this.performer = performer;
    this.hashCode = this.toString().hashCode();
    this.memoryCost = memoryCost;
    this.crossTask = true;
    this.innerSeqTask = false;
    createSummary();
  }

  protected AbstractCrossSpaceCompactionTask(
      String storageGroupName,
      String dataRegionId,
      long timePartition,
      TsFileManager tsFileManager,
      long serialId,
      CompactionTaskType compactionTaskType) {
    super(storageGroupName, dataRegionId, timePartition, tsFileManager, serialId, compactionTaskType);
  }

  public List<TsFileResource> getSelectedSequenceFiles() {
    return selectedSequenceFiles;
  }

  public List<TsFileResource> getSelectedUnsequenceFiles() {
    return selectedUnsequenceFiles;
  }

  @Override
  protected List<TsFileResource> getAllSourceTsFiles() {
    List<TsFileResource> allRelatedFiles = new ArrayList<>();
    allRelatedFiles.addAll(selectedSequenceFiles);
    allRelatedFiles.addAll(selectedUnsequenceFiles);
    return allRelatedFiles;
  }

  @Override
  public String toString() {
    return storageGroupName
        + "-"
        + dataRegionId
        + "-"
        + timePartition
        + " task seq files are "
        + selectedSequenceFiles.toString()
        + " , unseq files are "
        + selectedUnsequenceFiles.toString();
  }

  @Override
  public int hashCode() {
    return hashCode;
  }
}
