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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InPlaceCrossCompactionWriter extends FastCrossCompactionWriter {

  boolean[] deviceExistButNotRewrite;
  boolean[] hasNotRewrittenDevice;

  public InPlaceCrossCompactionWriter(
      List<TsFileResource> targetResources,
      List<TsFileResource> seqSourceResources,
      Map<TsFileResource, TsFileSequenceReader> readerMap)
      throws IOException {
    super(targetResources, seqSourceResources, readerMap, true);
    this.deviceExistButNotRewrite = new boolean[targetResources.size()];
    this.hasNotRewrittenDevice = new boolean[targetResources.size()];
  }

  public void writeChunkMetadataList(TsFileResource resource, List<ChunkMetadata> chunkMetadataList)
      throws IOException {
    for (int i = 0; i < targetFileWriters.size(); i++) {
      CompactionTsFileWriter targetFileWriter = this.targetFileWriters.get(i);
      if (targetFileWriter.getFile().equals(resource.getTsFile())) {
        for (ChunkMetadata chunkMetadata : chunkMetadataList) {
          targetFileWriter.directlyWriteChunkMetadata(chunkMetadata);
          deviceExistButNotRewrite[i] = true;
          hasNotRewrittenDevice[i] = true;
        }
        return;
      }
    }
  }

  @Override
  public void endChunkGroup() throws IOException {
    super.endChunkGroup();
    for (int i = 0; i < targetFileWriters.size(); i++) {
      if (deviceExistButNotRewrite[i]) {
        CompactionUtils.updateResource(targetResources.get(i), targetFileWriters.get(i), deviceId);
        targetFileWriters.get(i).endChunkGroup();
        deviceExistButNotRewrite[i] = false;
      }
    }
  }

  @Override
  public void endFile() throws IOException {
    for (int i = 0; i < isEmptyFile.length; i++) {
      targetFileWriters.get(i).endFile();
      // set empty target file to DELETED
      if (isEmptyFile[i] && !hasNotRewrittenDevice[i]) {
        targetResources.get(i).forceMarkDeleted();
      }
    }
  }
}
