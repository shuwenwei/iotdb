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

package org.apache.iotdb.db.storageengine.dataregion.compaction.selector.constant;

import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.IInnerSeqSpaceSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.NewSizeTieredCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.SizeTieredCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;

@SuppressWarnings("squid:S6548")
public enum InnerSequenceCompactionSelector {
  SIZE_TIERED,
  NEW_SIZE_TIERED;

  public static InnerSequenceCompactionSelector getInnerSequenceCompactionSelector(String name) {
    if (SIZE_TIERED.toString().equalsIgnoreCase(name)) {
      return SIZE_TIERED;
    }
    if (NEW_SIZE_TIERED.toString().equalsIgnoreCase(name)) {
      return NEW_SIZE_TIERED;
    }
    throw new IllegalCompactionSelectorNameException("Illegal Compaction Selector " + name);
  }

  @SuppressWarnings("squid:S1301")
  public IInnerSeqSpaceSelector createInstance(
      String storageGroupName,
      String dataRegionId,
      long timePartition,
      TsFileManager tsFileManager,
      CompactionScheduleContext context) {
    switch (this) {
      case NEW_SIZE_TIERED:
        return new NewSizeTieredCompactionSelector(
            storageGroupName, dataRegionId, timePartition, true, tsFileManager, context);
      case SIZE_TIERED:
      default:
        return new SizeTieredCompactionSelector(
            storageGroupName, dataRegionId, timePartition, true, tsFileManager);
    }
  }
}
