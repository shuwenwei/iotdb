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

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.InPlaceCompactionErrorException;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import java.io.IOException;

public class InPlaceCompactionUnSeqFile extends InPlaceCompactionFile {
  public InPlaceCompactionUnSeqFile(TsFileResource resource) {
    super(resource);
  }

  @Override
  public void revert() throws InPlaceCompactionErrorException {
    releaseLock();
    try {
      writeLock();
      deleteCompactionModsFile();
      tsFileResource.setStatus(TsFileResourceStatus.NORMAL);
    } catch (IOException e) {
      throw new InPlaceCompactionErrorException("error when recover source file", e);
    } finally {
      writeUnLock();
    }
  }

  @Override
  public void releaseResourceAndResetStatus() {
    // If the status of the TsFileResource is DELETED, this
    // method will fail
    tsFileResource.setStatus(TsFileResourceStatus.NORMAL);
    releaseLock();
  }
}
