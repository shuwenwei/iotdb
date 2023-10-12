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

import java.io.IOException;

public abstract class InPlaceCompactionFile {

  protected TsFileResource tsFileResource;
  protected InPlaceCompactionFileLockStatus lockStatus;

  public InPlaceCompactionFile(TsFileResource resource) {
    this.tsFileResource = resource;
    this.lockStatus = InPlaceCompactionFileLockStatus.NO_LOCK;
  }

  public TsFileResource getTsFileResource() {
    return tsFileResource;
  }

  // release held write/read lock and do some other cleanup things.
  public void releaseLock() {
    if (lockStatus == InPlaceCompactionFileLockStatus.READ_LOCK) {
      readUnLock();
    }
    if (lockStatus == InPlaceCompactionFileLockStatus.WRITE_LOCK) {
      writeUnLock();
    }
  }

  public abstract void revert() throws InPlaceCompactionErrorException;

  public void deleteCompactionModsFile() throws IOException {
    tsFileResource.getCompactionModFile().remove();
  }

  public void releaseAcquiredLockAndAcquireWriteLock() {
    releaseLock();
    writeLock();
  }

  public void releaseWriteLockAndReadLock() {
    writeUnLock();
    readLock();
  }

  public void writeLock() {
    tsFileResource.writeLock();
    lockStatus = InPlaceCompactionFileLockStatus.WRITE_LOCK;
  }

  public void writeUnLock() {
    tsFileResource.writeUnlock();
    lockStatus = InPlaceCompactionFileLockStatus.NO_LOCK;
  }

  public void readLock() {
    tsFileResource.readLock();
    lockStatus = InPlaceCompactionFileLockStatus.READ_LOCK;
  }

  public void readUnLock() {
    tsFileResource.readUnlock();
    lockStatus = InPlaceCompactionFileLockStatus.NO_LOCK;
  }

  public abstract void releaseResourceAndResetStatus() throws IOException;
}
