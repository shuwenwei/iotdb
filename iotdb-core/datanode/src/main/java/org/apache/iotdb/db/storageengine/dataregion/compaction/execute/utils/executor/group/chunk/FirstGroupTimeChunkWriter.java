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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.group.chunk;

import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.TimeStatistics;
import org.apache.iotdb.tsfile.write.chunk.TimeChunkWriter;
import org.apache.iotdb.tsfile.write.page.TimePageWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class FirstGroupTimeChunkWriter extends TimeChunkWriter {

  private List<CompactPagePlan> pageTimeRanges = new ArrayList<>();

  public FirstGroupTimeChunkWriter(
      String measurementId,
      CompressionType compressionType,
      TSEncoding encodingType,
      Encoder timeEncoder) {
    super(measurementId, compressionType, encodingType, timeEncoder);
  }

  @Override
  public void writePageToPageBuffer() {
    TimePageWriter pageWriter = getPageWriter();
    if (pageWriter != null && pageWriter.getPointNumber() > 0) {
      TimeStatistics statistics = pageWriter.getStatistics();
      pageTimeRanges.add(
          new CompactPagePlan(statistics.getStartTime(), statistics.getEndTime(), false));
      super.writePageToPageBuffer();
    }
  }

  @Override
  public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {
    super.writeToFileWriter(tsfileWriter);
    this.pageTimeRanges = new ArrayList<>();
  }

  @Override
  public void writePageHeaderAndDataIntoBuff(ByteBuffer data, PageHeader header)
      throws PageException {
    pageTimeRanges.add(new CompactPagePlan(header.getStartTime(), header.getEndTime(), true));
    super.writePageHeaderAndDataIntoBuff(data, header);
  }

  public List<CompactPagePlan> getPageTimeRanges() {
    return this.pageTimeRanges;
  }
}
