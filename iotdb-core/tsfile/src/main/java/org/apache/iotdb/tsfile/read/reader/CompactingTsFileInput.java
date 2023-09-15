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

package org.apache.iotdb.tsfile.read.reader;

import org.apache.commons.io.input.BoundedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class CompactingTsFileInput implements TsFileInput {

  private static final Logger logger = LoggerFactory.getLogger(CompactingTsFileInput.class);
  private final String filePath;
  private final FileChannel dataFileChannel;
  private final long dataSize;
  private final long metadataSize;
  private final FileChannel metadataFileChannel;

  public CompactingTsFileInput(Path dataFile, Path metadataFile) throws IOException {
    filePath = dataFile.toFile().getAbsolutePath();
    dataFileChannel = FileChannel.open(dataFile, StandardOpenOption.READ);
    dataSize = dataFileChannel.size();
    metadataFileChannel = FileChannel.open(metadataFile, StandardOpenOption.READ);
    metadataSize = metadataFileChannel.size();
  }

  @Override
  public long size() throws IOException {
    return dataSize + metadataSize;
  }

  @Override
  public long position() throws IOException {
    return dataFileChannel.position() + metadataFileChannel.position();
  }

  @Override
  public TsFileInput position(long newPosition) throws IOException {
    try {
      if (newPosition >= dataSize) {
        dataFileChannel.position(dataSize);
        metadataFileChannel.position(newPosition - dataSize);
      } else {
        dataFileChannel.position(newPosition);
        metadataFileChannel.position(0);
      }
      return this;
    } catch (IOException e) {
      logger.error("Error happened while changing {} position to {}", filePath, newPosition);
      throw e;
    }
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    try {
      if (this.position() >= dataSize) {
        return metadataFileChannel.read(dst);
      } else {
        return dataFileChannel.read(dst) + metadataFileChannel.read(dst);
      }
    } catch (ClosedByInterruptException e) {
      logger.warn(
          "Current thread is interrupted by another thread when it is blocked in an I/O operation upon a channel.");
      return -1;
    } catch (IOException e) {
      logger.error("Error happened while reading {} from current position", filePath);
      throw e;
    }
  }

  @Override
  public int read(ByteBuffer dst, long position) throws IOException {
    try {
      if (position >= dataSize) {
        return metadataFileChannel.read(dst, position - dataSize);
      } else {
        int readSize = dst.remaining();
        if (position + readSize <= dataSize) {
          return dataFileChannel.read(dst, position);
        }
        return dataFileChannel.read(dst, position) + metadataFileChannel.read(dst, 0);
      }
    } catch (ClosedByInterruptException e) {
      logger.warn(
          "Current thread is interrupted by another thread when it is blocked in an I/O operation upon a channel.");
      return -1;
    } catch (IOException e) {
      logger.error("Error happened while reading {} from position {}", filePath, position);
      throw e;
    }
  }

  @Override
  public InputStream wrapAsInputStream() {
    return new SequenceInputStream(
        new BoundedInputStream(Channels.newInputStream(dataFileChannel), dataSize),
        Channels.newInputStream(metadataFileChannel));
  }

  @Override
  public void close() throws IOException {
    try {
      dataFileChannel.close();
      metadataFileChannel.close();
    } catch (IOException e) {
      logger.error("Error happened while closing {}", filePath);
      throw e;
    }
  }

  @Override
  public String getFilePath() {
    return filePath;
  }
}
