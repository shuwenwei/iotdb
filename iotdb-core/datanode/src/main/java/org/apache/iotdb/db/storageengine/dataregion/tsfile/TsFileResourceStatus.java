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
package org.apache.iotdb.db.storageengine.dataregion.tsfile;

public enum TsFileResourceStatus {
  UNCLOSED,
  /** The resource in status NORMAL, COMPACTION_CANDIDATE, COMPACTING, DELETED is all CLOSED. */
  NORMAL,
  COMPACTION_CANDIDATE,
  COMPACTING,
  // Indicate that the TsFile is split into data part and meta part during compacting
  SPLIT_DURING_COMPACTING,
  /**
   * Indicating the TsFile is prepared to be hard linked. It is seemed as a write operation because
   * it cannot be modified by other module during hard linking
   */
  HARD_LINKING,
  DELETED
}
