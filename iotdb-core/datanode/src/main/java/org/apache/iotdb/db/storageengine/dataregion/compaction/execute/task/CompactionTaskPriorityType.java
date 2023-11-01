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

public enum CompactionTaskPriorityType {
  /** default compaction task type */
  NORMAL(10),

  /**
   * in either of the following situations: 1. the TsFile has .mods file whose size exceeds 50 MB.
   * 2. the TsFile has .mods file and the disk availability rate is lower than the
   * disk_space_warning_threshold.
   */
  MOD_SETTLE(20),

  /**
   * a task of this type is created when the valid information ratio of the TsFile is below a
   * certain value. Used to collate and merge invalid data in a file
   */
  IN_PLACE_SETTLE(30),

  /** use for cross InPlace compaction task */
  IN_PLACE(10);

  /** the larger the value, the sooner it is executed */
  final int executePriority;

  CompactionTaskPriorityType(int executePriority) {
    this.executePriority = executePriority;
  }

  public int getExecutePriority() {
    return executePriority;
  }
}
