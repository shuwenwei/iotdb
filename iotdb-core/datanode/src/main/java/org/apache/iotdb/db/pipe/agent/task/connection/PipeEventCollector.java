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

package org.apache.iotdb.db.pipe.agent.task.connection;

import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.progress.PipeEventCommitManager;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class PipeEventCollector implements EventCollector {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeEventCollector.class);

  private final UnboundedBlockingPendingQueue<Event> pendingQueue;

  private final long creationTime;

  private final int regionId;

  private final boolean forceTabletFormat;

  private final AtomicInteger collectInvocationCount = new AtomicInteger(0);
  private boolean hasNoGeneratedEvent = true;
  private boolean isFailedToIncreaseReferenceCount = false;

  public PipeEventCollector(
      final UnboundedBlockingPendingQueue<Event> pendingQueue,
      final long creationTime,
      final int regionId,
      final boolean forceTabletFormat) {
    this.pendingQueue = pendingQueue;
    this.creationTime = creationTime;
    this.regionId = regionId;
    this.forceTabletFormat = forceTabletFormat;
  }

  @Override
  public void collect(final Event event) {
    try {
      if (event instanceof PipeInsertNodeTabletInsertionEvent) {
        parseAndCollectEvent((PipeInsertNodeTabletInsertionEvent) event);
      } else if (event instanceof PipeRawTabletInsertionEvent) {
        parseAndCollectEvent((PipeRawTabletInsertionEvent) event);
      } else if (event instanceof PipeTsFileInsertionEvent) {
        parseAndCollectEvent((PipeTsFileInsertionEvent) event);
      } else if (event instanceof PipeDeleteDataNodeEvent) {
        collectEvent(event);
      } else if (!(event instanceof ProgressReportEvent)) {
        collectEvent(event);
      }
    } catch (final PipeException e) {
      throw e;
    } catch (final Exception e) {
      throw new PipeException("Error occurred when collecting events from processor.", e);
    }
  }

  private void parseAndCollectEvent(final PipeInsertNodeTabletInsertionEvent sourceEvent) {
    if (Objects.nonNull(sourceEvent)) {
      if (sourceEvent instanceof PipeInsertNodeTabletInsertionEvent) {
        try {
          PipeInsertNodeTabletInsertionEvent tabletInsertionEvent =
              (PipeInsertNodeTabletInsertionEvent) sourceEvent;
          final InsertNode insertNode = tabletInsertionEvent.getInsertNode();
          if (insertNode instanceof RelationalInsertTabletNode) {
            for (int i = 0; i < ((RelationalInsertTabletNode) insertNode).getRowCount(); i++) {
              LOGGER.warn(
                  "{}  processor insertNode println device {}",
                  sourceEvent.getPipeName(),
                  ((RelationalInsertTabletNode) insertNode).getDeviceID(i));
            }
          } else if (insertNode instanceof RelationalInsertRowsNode) {
            for (InsertRowNode rowNode :
                ((RelationalInsertRowsNode) insertNode).getInsertRowNodeList()) {
              LOGGER.warn(
                  "{} processor insertNode println device {}",
                  sourceEvent.getPipeName(),
                  ((RelationalInsertRowNode) rowNode).getDeviceID());
            }
          } else if (insertNode instanceof RelationalInsertRowNode) {
            LOGGER.warn(
                "{} processor insertNode println device {}",
                sourceEvent.getPipeName(),
                ((RelationalInsertRowNode) insertNode).getDeviceID());
          }
        } catch (Exception e) {
          LOGGER.error(e.getMessage());
        }
      }
    }
    if (sourceEvent.shouldParseTimeOrPattern()) {
      for (final PipeRawTabletInsertionEvent parsedEvent :
          sourceEvent.toRawTabletInsertionEvents()) {
        collectParsedRawTableEvent(parsedEvent);
      }
    } else {
      collectEvent(sourceEvent);
    }
  }

  private void parseAndCollectEvent(final PipeRawTabletInsertionEvent sourceEvent) {
    collectParsedRawTableEvent(
        sourceEvent.shouldParseTimeOrPattern()
            ? sourceEvent.parseEventWithPatternOrTime()
            : sourceEvent);
  }

  private void parseAndCollectEvent(final PipeTsFileInsertionEvent sourceEvent) throws Exception {
    if (!sourceEvent.waitForTsFileClose()) {
      LOGGER.warn(
          "Pipe skipping temporary TsFile which shouldn't be transferred: {}",
          sourceEvent.getTsFile());
      return;
    }

    if (!forceTabletFormat
        && (!sourceEvent.shouldParseTimeOrPattern()
            || (sourceEvent.isTableModelEvent()
                && sourceEvent.getTablePattern() == null
                && !sourceEvent.shouldParseTime()))) {
      if (sourceEvent instanceof PipeTsFileInsertionEvent
          && (((PipeTsFileInsertionEvent) sourceEvent).isTableModelEvent())) {
        try (final TsFileSequenceReader reader =
            new TsFileSequenceReader(
                (((PipeTsFileInsertionEvent) sourceEvent).getTsFile()).getAbsolutePath())) {
          final TsFileSequenceReaderTimeseriesMetadataIterator timeseriesMetadataIterator =
              new TsFileSequenceReaderTimeseriesMetadataIterator(reader, true, 1);
          while (timeseriesMetadataIterator.hasNext()) {
            final Map<IDeviceID, List<TimeseriesMetadata>> device2TimeseriesMetadata =
                timeseriesMetadataIterator.next();

            for (IDeviceID deviceId : device2TimeseriesMetadata.keySet()) {
              LOGGER.warn(
                  "{}  processor load tsfile println device {}",
                  sourceEvent.getPipeName(),
                  deviceId);
            }
          }
        } catch (Exception e) {
          LOGGER.error(e.getMessage());
        }
      } else collectEvent(sourceEvent);
      return;
    }

    try {
      for (final TabletInsertionEvent parsedEvent : sourceEvent.toTabletInsertionEvents()) {
        collectParsedRawTableEvent((PipeRawTabletInsertionEvent) parsedEvent);
      }
    } finally {
      sourceEvent.close();
    }
  }

  private void collectParsedRawTableEvent(final PipeRawTabletInsertionEvent parsedEvent) {
    if (!parsedEvent.hasNoNeedParsingAndIsEmpty()) {
      hasNoGeneratedEvent = false;
      try {
        final Tablet tablet = ((PipeRawTabletInsertionEvent) (parsedEvent)).convertToTablet();
        for (int i = 0; i < tablet.getRowSize(); i++) {
          LOGGER.warn(
              "{}  processor rawTablet println device {}",
              parsedEvent.getPipeName(),
              tablet.getDeviceID(i));
        }
      } catch (Exception e) {
        LOGGER.error(e.getMessage());
      }
      collectEvent(parsedEvent);
    }
  }

  private void collectEvent(final Event event) {
    if (event instanceof EnrichedEvent) {
      if (!((EnrichedEvent) event).increaseReferenceCount(PipeEventCollector.class.getName())) {
        LOGGER.warn("PipeEventCollector: The event {} is already released, skipping it.", event);
        isFailedToIncreaseReferenceCount = true;
        return;
      }

      // Assign a commit id for this event in order to report progress in order.
      PipeEventCommitManager.getInstance()
          .enrichWithCommitterKeyAndCommitId((EnrichedEvent) event, creationTime, regionId);

      // Assign a rebootTime for pipeConsensus
      ((EnrichedEvent) event).setRebootTimes(PipeDataNodeAgent.runtime().getRebootTimes());
    }

    if (event instanceof PipeHeartbeatEvent) {
      ((PipeHeartbeatEvent) event).recordConnectorQueueSize(pendingQueue);
    }

    pendingQueue.directOffer(event);
    collectInvocationCount.incrementAndGet();
  }

  public void resetFlags() {
    collectInvocationCount.set(0);
    hasNoGeneratedEvent = true;
    isFailedToIncreaseReferenceCount = false;
  }

  public long getCollectInvocationCount() {
    return collectInvocationCount.get();
  }

  public boolean hasNoCollectInvocationAfterReset() {
    return collectInvocationCount.get() == 0;
  }

  public boolean hasNoGeneratedEvent() {
    return hasNoGeneratedEvent;
  }

  public boolean isFailedToIncreaseReferenceCount() {
    return isFailedToIncreaseReferenceCount;
  }
}
