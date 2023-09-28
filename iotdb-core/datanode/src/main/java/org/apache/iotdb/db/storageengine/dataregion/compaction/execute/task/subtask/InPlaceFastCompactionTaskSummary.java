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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask;

public class InPlaceFastCompactionTaskSummary extends FastCompactionTaskSummary {
  private int chunkGroupNoneOverlap = 0;
  private int rewriteChunkGroupNum = 0;
  private long splitMetadataSize = 0;

  public void setChunkGroupNoneOverlap(int chunkGroupNoneOverlap) {
    this.chunkGroupNoneOverlap = chunkGroupNoneOverlap;
  }

  public void setRewriteChunkGroupNum(int rewriteChunkGroupNum) {
    this.rewriteChunkGroupNum = rewriteChunkGroupNum;
  }

  public void setSplitMetadataSize(long splitMetadataSize) {
    this.splitMetadataSize = splitMetadataSize;
  }

  @Override
  public String toString() {
    return String.format(
        "CHUNK_GROUP_NONE_OVERLAP num is %d, CHUNK_GROUP_OVERLAP num is %d"
            + " CHUNK_NONE_OVERLAP num is %d, CHUNK_NONE_OVERLAP_BUT_DESERIALIZE num is %d,"
            + " CHUNK_OVERLAP_OR_MODIFIED num is %d, PAGE_NONE_OVERLAP num is %d,"
            + " PAGE_NONE_OVERLAP_BUT_DESERIALIZE num is %d, PAGE_OVERLAP_OR_MODIFIED num is %d,"
            + " PAGE_FAKE_OVERLAP num is %d."
            + " SPLIT_METADATA_SIZE is %d.",
        chunkGroupNoneOverlap,
        rewriteChunkGroupNum,
        chunkNoneOverlap,
        chunkNoneOverlapButDeserialize,
        chunkOverlapOrModified,
        pageNoneOverlap,
        pageNoneOverlapButDeserialize,
        pageOverlapOrModified,
        pageFakeOverlap,
        splitMetadataSize);
  }
}
