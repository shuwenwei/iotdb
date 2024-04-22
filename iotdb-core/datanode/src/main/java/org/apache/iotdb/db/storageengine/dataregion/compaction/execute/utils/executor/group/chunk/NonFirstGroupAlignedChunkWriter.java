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

import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ValueChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class NonFirstGroupAlignedChunkWriter extends AlignedChunkWriterImpl {
  private int currentPage = 0;
  private CompactedChunkRecord compactedChunkRecord;

  public NonFirstGroupAlignedChunkWriter(IMeasurementSchema timeSchema, List<IMeasurementSchema> valueSchemaList, CompactedChunkRecord compactedChunkRecord) {
    timeChunkWriter =
        new FirstGroupTimeChunkWriter(
            timeSchema.getMeasurementId(),
            timeSchema.getCompressor(),
            timeSchema.getEncodingType(),
            timeSchema.getTimeEncoder());

    valueChunkWriterList = new ArrayList<>(valueSchemaList.size());
    for (int i = 0; i < valueSchemaList.size(); i++) {
      valueChunkWriterList.add(
          new ValueChunkWriter(
              valueSchemaList.get(i).getMeasurementId(),
              valueSchemaList.get(i).getCompressor(),
              valueSchemaList.get(i).getType(),
              valueSchemaList.get(i).getEncodingType(),
              valueSchemaList.get(i).getValueEncoder()));
    }
    this.valueIndex = 0;
    this.remainingPointsNumber = timeChunkWriter.getRemainingPointNumberForCurrentPage();
    this.compactedChunkRecord = compactedChunkRecord;
  }

  @Override
  protected boolean checkPageSizeAndMayOpenANewPage() {
    long endTime = timeChunkWriter.getPageWriter().getStatistics().getEndTime();
    return endTime == compactedChunkRecord.getPageRecords().get(currentPage).getTimeRange().getMax();
  }

  @Override
  protected void writePageToPageBuffer() {
    super.writePageToPageBuffer();
    currentPage++;
  }

  @Override
  public void writePageHeaderAndDataIntoTimeBuff(ByteBuffer data, PageHeader header) throws PageException {
    super.writePageHeaderAndDataIntoTimeBuff(data, header);
    currentPage++;
  }

  @Override
  public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {
    for (ValueChunkWriter valueChunkWriter : valueChunkWriterList) {
      valueChunkWriter.writeToFileWriter(tsfileWriter);
    }
  }

  @Override
  public boolean checkIsChunkSizeOverThreshold(long size, long pointNum, boolean returnTrueIfChunkEmpty) {
    return currentPage >= compactedChunkRecord.getPageRecords().size();
  }

  public int getCurrentPage() {
    return currentPage;
  }
}
