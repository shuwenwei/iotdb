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

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ValueChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import java.util.ArrayList;
import java.util.List;

public class FirstGroupAlignedChunkWriter extends AlignedChunkWriterImpl {

  // TestOnly
  public FirstGroupAlignedChunkWriter(VectorMeasurementSchema schema) {
    timeChunkWriter =
        new FirstGroupTimeChunkWriter(
            schema.getMeasurementId(),
            schema.getCompressor(),
            schema.getTimeTSEncoding(),
            schema.getTimeEncoder());

    List<String> valueMeasurementIdList = schema.getSubMeasurementsList();
    List<TSDataType> valueTSDataTypeList = schema.getSubMeasurementsTSDataTypeList();
    List<TSEncoding> valueTSEncodingList = schema.getSubMeasurementsTSEncodingList();
    List<Encoder> valueEncoderList = schema.getSubMeasurementsEncoderList();

    valueChunkWriterList = new ArrayList<>(valueMeasurementIdList.size());
    for (int i = 0; i < valueMeasurementIdList.size(); i++) {
      valueChunkWriterList.add(
          new ValueChunkWriter(
              valueMeasurementIdList.get(i),
              schema.getCompressor(),
              valueTSDataTypeList.get(i),
              valueTSEncodingList.get(i),
              valueEncoderList.get(i)));
    }

    this.valueIndex = 0;
    this.remainingPointsNumber = timeChunkWriter.getRemainingPointNumberForCurrentPage();
  }

  public FirstGroupAlignedChunkWriter(
      IMeasurementSchema timeSchema, List<IMeasurementSchema> valueSchemaList) {
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
  }

  public FirstGroupAlignedChunkWriter(List<IMeasurementSchema> schemaList) {
    TSEncoding timeEncoding =
        TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
    TSDataType timeType = TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType();
    CompressionType timeCompression = TSFileDescriptor.getInstance().getConfig().getCompressor();
    timeChunkWriter =
        new FirstGroupTimeChunkWriter(
            "",
            timeCompression,
            timeEncoding,
            TSEncodingBuilder.getEncodingBuilder(timeEncoding).getEncoder(timeType));

    valueChunkWriterList = new ArrayList<>(schemaList.size());
    for (int i = 0; i < schemaList.size(); i++) {
      valueChunkWriterList.add(
          new ValueChunkWriter(
              schemaList.get(i).getMeasurementId(),
              schemaList.get(i).getCompressor(),
              schemaList.get(i).getType(),
              schemaList.get(i).getEncodingType(),
              schemaList.get(i).getValueEncoder()));
    }

    this.valueIndex = 0;
    this.remainingPointsNumber = timeChunkWriter.getRemainingPointNumberForCurrentPage();
  }

  public CompactedChunkRecord getCompactedChunkRecord() {
    return new CompactedChunkRecord(
        ((FirstGroupTimeChunkWriter) this.getTimeChunkWriter()).getPageTimeRanges());
  }
}
