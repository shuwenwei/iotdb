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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.IllegalCompactionTaskSummaryException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionPerformerSubTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.FastDeviceCrossCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.reader.CompactingTsFileInput;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class FastDeviceCompactionPerformer implements ICrossCompactionPerformer {

  private List<TsFileResource> seqFiles;
  private List<TsFileResource> unseqFiles;
  private FastCompactionTaskSummary subTaskSummary;
  private static final int SUB_TASK_NUM =
      IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();
  private Map<TsFileResource, List<Modification>> modificationCache = new ConcurrentHashMap<>();

  private Map<TsFileResource, DeviceTimeIndex> deviceTimeIndexMap;
  private Map<TsFileResource, TsFileSequenceReader> readerCacheMap;

  public FastDeviceCompactionPerformer(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    this.seqFiles = seqFiles;
    this.unseqFiles = unseqFiles;
    this.deviceTimeIndexMap = new HashMap<>();
    this.readerCacheMap = new HashMap<>();
  }

  @Override
  public void perform() throws Exception {
    buildDeviceTimeIndexList(seqFiles);
    buildDeviceTimeIndexList(unseqFiles);

    moveMetadataToTempFile(seqFiles);

    // todo: use special TsFileInput to generate readerCacheMap

    initReaderCacheMap();

    try (MultiTsFileDeviceIterator deviceIterator =
        new MultiTsFileDeviceIterator(seqFiles, unseqFiles, readerCacheMap)) {
      FastDeviceCrossCompactionWriter compactionWriter =
          new FastDeviceCrossCompactionWriter(seqFiles, seqFiles, readerCacheMap);
      while (deviceIterator.hasNextDevice()) {
        checkThreadInterrupt();

        Pair<String, Boolean> deviceIsAlignedPair = deviceIterator.nextDevice();
        String device = deviceIsAlignedPair.left;
        boolean isAligned = deviceIsAlignedPair.right;
        compactionWriter.startChunkGroup(device, isAligned);

        List<TsFileResource> sortedUnseqFilesOfCurrentDevice =
            unseqFiles.stream()
                .filter(resource -> !deviceTimeIndexMap.get(resource).definitelyNotContains(device))
                .sorted(
                    Comparator.comparingLong(
                        resource -> deviceTimeIndexMap.get(resource).getStartTime(device)))
                .collect(Collectors.toList());
        List<TsFileResource> sortedSeqFilesOfCurrentDevice =
            seqFiles.stream()
                .filter(resource -> !deviceTimeIndexMap.get(resource).definitelyNotContains(device))
                .sorted(
                    Comparator.comparingLong(
                        resource -> deviceTimeIndexMap.get(resource).getStartTime(device)))
                .collect(Collectors.toList());

        if (sortedUnseqFilesOfCurrentDevice.isEmpty()) {
          // todo: update chunkMetadata
          copyDeviceChunkMetadata(compactionWriter, sortedSeqFilesOfCurrentDevice, device);
          compactionWriter.endChunkGroup();
          // check whether to flush chunk metadata or not
          compactionWriter.checkAndMayFlushChunkMetadata();
          // Add temp file metrics
          subTaskSummary.setTemporalFileSize(compactionWriter.getWriterSize());
          continue;
        }

        // todo: select overlap files of current device

        sortedSeqFilesOfCurrentDevice.addAll(sortedUnseqFilesOfCurrentDevice);
        sortedSeqFilesOfCurrentDevice.sort(Comparator.comparingLong(resource -> deviceTimeIndexMap.get(resource).getStartTime(device)));
        if (isAligned) {
          compactAlignedSeries(
              device, deviceIterator, compactionWriter, sortedSeqFilesOfCurrentDevice);
        } else {
          compactNonAlignedSeries(
              device, deviceIterator, compactionWriter, sortedSeqFilesOfCurrentDevice);
        }

        compactionWriter.endChunkGroup();
        // check whether to flush chunk metadata or not
        compactionWriter.checkAndMayFlushChunkMetadata();
        // Add temp file metrics
        subTaskSummary.setTemporalFileSize(compactionWriter.getWriterSize());
      }
      compactionWriter.endFile();
      CompactionUtils.updatePlanIndexes(Collections.emptyList(), seqFiles, unseqFiles);
    } catch (IOException e) {
      // todo
      e.printStackTrace();
    } finally {
      // todo
    }
  }

  private List<DeviceTimeIndex> buildDeviceTimeIndexList(List<TsFileResource> resources)
      throws IOException {
    List<DeviceTimeIndex> deviceTimeIndexList = new ArrayList<>(resources.size());
    for (TsFileResource resource : resources) {
      DeviceTimeIndex deviceTimeIndex = getDeviceTimeIndex(resource);
      deviceTimeIndexList.add(deviceTimeIndex);
    }
    return deviceTimeIndexList;
  }

  private void moveMetadataToTempFile(List<TsFileResource> resources) throws IOException {
    for (TsFileResource resource : resources) {
      File sourceTsFile = resource.getTsFile();
      File metadataFile = new File(sourceTsFile.getAbsolutePath() + ".mt");
      Files.createFile(metadataFile.toPath());
      moveMetadataToTempFile(sourceTsFile, metadataFile);
    }
  }

  private void moveMetadataToTempFile(File srcFile, File dstFile) {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(srcFile.getAbsolutePath());
        FileChannel srcChannel =
            FileChannel.open(srcFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
        FileChannel dstChannel = FileChannel.open(dstFile.toPath(), StandardOpenOption.WRITE)) {
      long fileSize = srcChannel.size();
      long metadataSize = reader.getAllMetadataSize();
      long transferSize = srcChannel.transferTo(fileSize - metadataSize, metadataSize, dstChannel);
      if (transferSize != metadataSize) {
        throw new RuntimeException();
      }
      dstChannel.force(true);
      // release read lock
      // acquire write lock
      srcChannel.truncate(fileSize - metadataSize);
      srcChannel.force(true);
      // update status separate-metadata
      // release write lock

      // acquire read lock
    } catch (IOException e) {
      // todo
      e.printStackTrace();
    }
  }

  private void initReaderCacheMap() throws IOException {
    for (TsFileResource resource : seqFiles) {
      File dataFile = resource.getTsFile();
      File metadataFile = new File(dataFile.getAbsolutePath() + ".mt");

      CompactingTsFileInput tsFileInput =
          new CompactingTsFileInput(dataFile.toPath(), metadataFile.toPath());

      TsFileSequenceReader reader = new CompactionTsFileReader(tsFileInput, CompactionType.CROSS_COMPACTION);
      readerCacheMap.put(resource, reader);
    }
    for (TsFileResource resource : unseqFiles) {
      readerCacheMap.put(
          resource, new TsFileSequenceReader(resource.getTsFile().getAbsolutePath()));
    }
  }

  private void copyDeviceChunkMetadata(
      FastDeviceCrossCompactionWriter compactionWriter,
      List<TsFileResource> resources,
      String device)
      throws IOException {
    for (TsFileResource resource : resources) {
      Map<String, List<ChunkMetadata>> measurementChunkMetadataListMap =
          readerCacheMap.get(resource).readChunkMetadataInDevice(device);
      for (Map.Entry<String, List<ChunkMetadata>> measurementChunkMetadataList :
          measurementChunkMetadataListMap.entrySet()) {
        List<ChunkMetadata> chunkMetadataList = measurementChunkMetadataList.getValue();
        if (chunkMetadataList == null || chunkMetadataList.isEmpty()) {
          continue;
        }
        compactionWriter.writeChunkMetadataList(resource, measurementChunkMetadataList.getValue());
      }
      compactionWriter.checkAndMayFlushChunkMetadata();
    }
  }

  private void compactNonAlignedSeries(
      String device,
      MultiTsFileDeviceIterator deviceIterator,
      AbstractCompactionWriter compactionWriter,
      List<TsFileResource> sortedSourceFiles)
      throws IOException {
    // measurement -> tsfile resource -> timeseries metadata <startOffset, endOffset>
    // Get all measurements of the current device. Also get start offset and end offset of each
    // timeseries metadata, in order to facilitate the reading of chunkMetadata directly by this
    // offset later. Here we don't need to deserialize chunk metadata, we can deserialize them and
    // get their schema later.
    Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap =
        deviceIterator.getTimeseriesMetadataOffsetOfCurrentDevice();

    List<String> allMeasurements = new ArrayList<>(timeseriesMetadataOffsetMap.keySet());
    allMeasurements.sort((String::compareTo));

    int subTaskNums = Math.min(allMeasurements.size(), SUB_TASK_NUM);

    // assign all measurements to different sub tasks
    List<String>[] measurementsForEachSubTask = new ArrayList[subTaskNums];
    for (int idx = 0; idx < allMeasurements.size(); idx++) {
      if (measurementsForEachSubTask[idx % subTaskNums] == null) {
        measurementsForEachSubTask[idx % subTaskNums] = new ArrayList<>();
      }
      measurementsForEachSubTask[idx % subTaskNums].add(allMeasurements.get(idx));
    }

    // construct sub tasks and start compacting measurements in parallel
    List<Future<Void>> futures = new ArrayList<>();
    List<FastCompactionTaskSummary> taskSummaryList = new ArrayList<>();
    for (int i = 0; i < subTaskNums; i++) {
      FastCompactionTaskSummary taskSummary = new FastCompactionTaskSummary();
      futures.add(
          CompactionTaskManager.getInstance()
              .submitSubTask(
                  new FastCompactionPerformerSubTask(
                      compactionWriter,
                      timeseriesMetadataOffsetMap,
                      readerCacheMap,
                      modificationCache,
                      sortedSourceFiles,
                      measurementsForEachSubTask[i],
                      device,
                      taskSummary,
                      i)));
      taskSummaryList.add(taskSummary);
    }

    // wait for all sub tasks to finish
    for (int i = 0; i < subTaskNums; i++) {
      try {
        futures.get(i).get();
        subTaskSummary.increase(taskSummaryList.get(i));
      } catch (ExecutionException e) {
        throw new IOException("[Compaction] SubCompactionTask meet errors ", e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void compactAlignedSeries(
      String device,
      MultiTsFileDeviceIterator deviceIterator,
      AbstractCompactionWriter compactionWriter,
      List<TsFileResource> sortedSourceFiles)
      throws IOException, PageException, IllegalPathException, WriteProcessException {
    // measurement -> tsfile resource -> timeseries metadata <startOffset, endOffset>, including
    // empty value chunk metadata
    Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap =
        new LinkedHashMap<>();
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();

    // Get all value measurements and their schemas of the current device. Also get start offset and
    // end offset of each timeseries metadata, in order to facilitate the reading of chunkMetadata
    // directly by this offset later. Instead of deserializing chunk metadata later, we need to
    // deserialize chunk metadata here to get the schemas of all value measurements, because we
    // should get schemas of all value measurement to startMeasruement() and compaction process is
    // to read a batch of overlapped files each time, and we cannot make sure if the first batch of
    // overlapped tsfiles contain all the value measurements.
    for (Map.Entry<String, Pair<MeasurementSchema, Map<TsFileResource, Pair<Long, Long>>>> entry :
        deviceIterator.getTimeseriesSchemaAndMetadataOffsetOfCurrentDevice().entrySet()) {
      measurementSchemas.add(entry.getValue().left);
      timeseriesMetadataOffsetMap.put(entry.getKey(), entry.getValue().right);
    }

    FastCompactionTaskSummary taskSummary = new FastCompactionTaskSummary();
    new FastCompactionPerformerSubTask(
            compactionWriter,
            timeseriesMetadataOffsetMap,
            readerCacheMap,
            modificationCache,
            sortedSourceFiles,
            measurementSchemas,
            device,
            taskSummary)
        .call();
    subTaskSummary.increase(taskSummary);
  }

  @Override
  public void setTargetFiles(List<TsFileResource> targetFiles) {}

  @Override
  public void setSummary(CompactionTaskSummary summary) {
    if (!(summary instanceof FastCompactionTaskSummary)) {
      throw new IllegalCompactionTaskSummaryException(
          "CompactionTaskSummary for FastCompactionPerformer "
              + "should be FastCompactionTaskSummary");
    }
    this.subTaskSummary = (FastCompactionTaskSummary) summary;
  }

  @Override
  public void setSourceFiles(List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    this.seqFiles = seqFiles;
    this.unseqFiles = unseqFiles;
  }

  private void checkThreadInterrupt() throws InterruptedException {
    if (Thread.interrupted() || subTaskSummary.isCancel()) {
      throw new InterruptedException(
          String.format("[Compaction] compaction for target file %s abort", seqFiles.toString()));
    }
  }

  private DeviceTimeIndex getDeviceTimeIndex(TsFileResource resource) throws IOException {
    if (deviceTimeIndexMap.containsKey(resource)) {
      return deviceTimeIndexMap.get(resource);
    }
    ITimeIndex timeIndex = resource.getTimeIndex();
    if (timeIndex instanceof FileTimeIndex) {
      DeviceTimeIndex deviceTimeIndex = resource.buildDeviceTimeIndex();
      deviceTimeIndexMap.put(resource, deviceTimeIndex);
      return deviceTimeIndex;
    } else {
      deviceTimeIndexMap.put(resource, (DeviceTimeIndex) timeIndex);
      return (DeviceTimeIndex) timeIndex;
    }
  }
}
