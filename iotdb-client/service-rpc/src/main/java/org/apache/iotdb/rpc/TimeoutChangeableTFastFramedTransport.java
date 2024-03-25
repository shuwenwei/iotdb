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

package org.apache.iotdb.rpc;

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import java.net.SocketException;

public class TimeoutChangeableTFastFramedTransport extends TElasticFramedTransport
    implements TimeoutChangeableTransport {

  private final TSocket underlyingSocket;

  public TimeoutChangeableTFastFramedTransport(
      TSocket underlying, int thriftDefaultBufferSize, int thriftMaxFrameSize, boolean copyBinary) {
    super(underlying, thriftDefaultBufferSize, thriftMaxFrameSize, copyBinary);
    this.underlyingSocket = underlying;
  }

  @Override
  public void setTimeout(int timeout) {
    underlyingSocket.setTimeout(timeout);
  }

  @Override
  public int getTimeOut() throws SocketException {
    return underlyingSocket.getSocket().getSoTimeout();
  }

  @Override
  public TTransport getSocket() {
    // in fact, this should be the same with underlying...
    return underlyingSocket;
  }

  public static class Factory extends TTransportFactory {

    private final int thriftDefaultBufferSize;
    protected final int thriftMaxFrameSize;
    protected final boolean copyBinary;

    public Factory(int thriftDefaultBufferSize, int thriftMaxFrameSize, boolean copyBinary) {
      this.thriftDefaultBufferSize = thriftDefaultBufferSize;
      this.thriftMaxFrameSize = thriftMaxFrameSize;
      this.copyBinary = copyBinary;
    }

    @Override
    public TTransport getTransport(TTransport trans) {
      if (trans instanceof TSocket) {
        return new TimeoutChangeableTFastFramedTransport(
            (TSocket) trans, thriftDefaultBufferSize, thriftMaxFrameSize, copyBinary);
      } else {
        return new TElasticFramedTransport(
            trans, thriftDefaultBufferSize, thriftMaxFrameSize, copyBinary);
      }
    }
  }
}
