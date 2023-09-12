package org.apache.iotdb.db.storageengine.dataregion.compaction;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

public class TempCompactionTest extends AbstractCompactionTest {
  @Before
  public void setUp()
      throws IOException, InterruptedException, MetadataException, WriteProcessException {
    super.setUp();
  }

  @After
  public void tearDown() throws StorageEngineException, IOException {
    super.tearDown();
  }

  @Test
  public void test0() throws IOException {
    File dir = new File("/Users/shuww/Downloads/fast");
    // get all seq files under the time partition dir
    List<File> tsFiles =
        Arrays.asList(
            Objects.requireNonNull(dir.listFiles(file -> file.getName().endsWith(TSFILE_SUFFIX))));
    // sort the seq files with timestamp
    tsFiles.sort(
        (f1, f2) -> {
          int timeDiff =
              Long.compareUnsigned(
                  Long.parseLong(f1.getName().split("-")[0]),
                  Long.parseLong(f2.getName().split("-")[0]));
          return timeDiff == 0
              ? Long.compareUnsigned(
                  Long.parseLong(f1.getName().split("-")[1]),
                  Long.parseLong(f2.getName().split("-")[1]))
              : timeDiff;
        });

    List<TsFileResource> resources = new ArrayList<>();
    for (File tsFile : tsFiles) {
      TsFileResource resource = new TsFileResource();
      resource.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
      resource.setFile(tsFile);
      resources.add(resource);
      resource.deserialize();
    }
    //    try (FastCompactionInnerCompactionEstimator estimator =
    //        new FastCompactionInnerCompactionEstimator()) {
    //      long mem = estimator.estimateInnerCompactionMemory(resources);
    //      System.out.println(mem);
    //    }

    //    SizeTieredCompactionSelector selector = new SizeTieredCompactionSelector("root.testsg",
    // "0", 0, true, tsFileManager);
    //    List<TsFileResource> selectedSeqResource =
    // selector.selectInnerSpaceTask(resources).get(0);

    InnerSpaceCompactionTask task1 =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            resources.subList(0, 30),
            true,
            new ReadChunkCompactionPerformer(),
            new AtomicInteger(0),
            0);
    task1.checkValidAndSetMerging();
  }

  @Test
  public void tes() {
    TsFileResource resource = new TsFileResource();
    resource.setFile(new File("/Users/shuww/wrong-file/0-19-0-1.tsfile"));
    System.out.println(CompactionUtils.validateSingleTsFiles(resource));
  }

  public boolean validate(List<TsFileResource> resources) throws IOException {
    // deviceID -> <TsFileResource, last end time>
    Map<String, Pair<TsFileResource, Long>> lastEndTimeMap = new HashMap<>();
    for (TsFileResource resource : resources) {
      DeviceTimeIndex timeIndex;
      if (resource.getTimeIndexType() != 1) {
        // if time index is not device time index, then deserialize it from resource file
        timeIndex = resource.buildDeviceTimeIndex();
      } else {
        timeIndex = (DeviceTimeIndex) resource.getTimeIndex();
      }
      Set<String> devices = timeIndex.getDevices();
      for (String device : devices) {
        long currentStartTime = timeIndex.getStartTime(device);
        long currentEndTime = timeIndex.getEndTime(device);
        Pair<TsFileResource, Long> lastDeviceInfo =
            lastEndTimeMap.computeIfAbsent(device, x -> new Pair<>(null, Long.MIN_VALUE));
        long lastEndTime = lastDeviceInfo.right;
        if (lastEndTime >= currentStartTime) {
          System.out.println(
              lastDeviceInfo.left.getTsFile().getName()
                  + " overlap with "
                  + resource.getTsFile().getName());
          return false;
        }
        lastDeviceInfo.left = resource;
        lastDeviceInfo.right = currentEndTime;
        lastEndTimeMap.put(device, lastDeviceInfo);
      }
    }
    return true;
  }

  public void test2() throws IOException {
    String path = "/Users/shuww/Downloads/region_2/2/2794/1689851435018-274-0-0.tsfile";
    String device = "root.NS.FDTY.HNSZNSFDTY0204F030";
    try (TsFileSequenceReader reader = new TsFileSequenceReader(path)) {
      Map<String, List<TimeseriesMetadata>> allTimeseriesMetadata =
          reader.getAllTimeseriesMetadata(true);
      List<TimeseriesMetadata> timeseriesMetadataList = allTimeseriesMetadata.get(device);
      for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataList) {
        if (timeseriesMetadata.getStatistics().getStartTime() != 1689850597328L) {
          continue;
        }
        for (IChunkMetadata chunkMetadata : timeseriesMetadata.getChunkMetadataList()) {
          if (chunkMetadata.getStartTime() != 1689850597328L) {
            continue;
          }
          Chunk chunk = reader.readMemChunk((ChunkMetadata) chunkMetadata);
          ChunkReader chunkReader = new ChunkReader(chunk);
          ChunkHeader chunkHeader = chunk.getHeader();
          ByteBuffer chunkDataBuffer = chunk.getData();
          while (chunkDataBuffer.remaining() > 0) {
            PageHeader pageHeader;
            if (((byte) (chunkHeader.getChunkType() & 0x3F))
                == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
              pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunk.getChunkStatistic());
            } else {
              pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
            }

            if (pageHeader.getStartTime() == 1689850597328L) {
              System.out.println(chunkHeader.getMeasurementID());
            }
          }
        }
      }
    }
  }
}
