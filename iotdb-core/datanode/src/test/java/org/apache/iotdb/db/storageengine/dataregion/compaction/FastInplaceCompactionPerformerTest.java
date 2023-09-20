package org.apache.iotdb.db.storageengine.dataregion.compaction;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastDeviceCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class FastInplaceCompactionPerformerTest extends AbstractCompactionTest {

  @Test
  public void test4() throws IOException {
    List<TsFileResource> seqFiles = new ArrayList<>();

    List<TsFileResource> unseqFiles = new ArrayList<>();
    TsFileResource seqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(10, 20)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    TsFileResource seqResource2 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(30, 50)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);

    TsFileResource seqResource3 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(70, 80)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);

    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d2",
            "s1",
            new TimeRange[] {new TimeRange(21, 24)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            false);

    TsFileResource unseqResource2 =
        generateSingleNonAlignedSeriesFile(
            "d2",
            "s1",
            new TimeRange[] {new TimeRange(25, 60)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            false);

    TsFileResource unseqResource3 =
        generateSingleNonAlignedSeriesFile(
            "d2",
            "s1",
            new TimeRange[] {new TimeRange(90, 100)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            false);

    seqFiles.add(seqResource1);
    seqFiles.add(seqResource2);
    seqFiles.add(seqResource3);
    unseqFiles.add(unseqResource1);
    unseqFiles.add(unseqResource2);
    unseqFiles.add(unseqResource3);

    CompactionTaskManager.getInstance().start();
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqFiles,
            unseqFiles,
            new FastDeviceCompactionPerformer(),
            new AtomicInteger(0),
            0,
            0);
    task.start();

    System.out.println();
  }

  @Test
  public void test1() throws IOException {
    List<TsFileResource> seqFiles = new ArrayList<>();

    List<TsFileResource> unseqFiles = new ArrayList<>();
    TsFileResource seqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(10, 20)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    TsFileResource seqResource2 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(30, 50)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);

    TsFileResource seqResource3 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(70, 80)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);

    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(21, 24)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            false);

    TsFileResource unseqResource2 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(25, 60)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            false);

    TsFileResource unseqResource3 =
        generateSingleNonAlignedSeriesFile(
            "d1",
            "s1",
            new TimeRange[] {new TimeRange(90, 100)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            false);

    seqFiles.add(seqResource1);
    seqFiles.add(seqResource2);
    seqFiles.add(seqResource3);
    unseqFiles.add(unseqResource1);
    unseqFiles.add(unseqResource2);
    unseqFiles.add(unseqResource3);

    CompactionTaskManager.getInstance().start();
//    AbstractCompactionTask task =
//        new InplaceCrossSpaceCompactionTask(
//            0,
//            tsFileManager,
//            seqFiles,
//            unseqFiles,
//            new FastDeviceCompactionPerformer(),
//            new AtomicInteger(0),
//            0,
//            0);
//    task.start();start

    System.out.println();
  }

  @Test
  public void test2() {
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(
            "/home/sww/source-codes/iotdb/iotdb-core/datanode/target/data/sequence/root.testsg/0/0/0-18-0-0.tsfile")) {
      final Map<String, List<ChunkMetadata>> deviceChunkMetadataMap =
          reader.readChunkMetadataInDevice("root.testsg.d1");
      for (Map.Entry<String, List<ChunkMetadata>> measurementChunkMetadataListEntry :
          deviceChunkMetadataMap.entrySet()) {
        System.out.println(measurementChunkMetadataListEntry.getKey());
        for (ChunkMetadata chunkMetadata : measurementChunkMetadataListEntry.getValue()) {
          System.out.println(chunkMetadata);

          final Chunk chunk = reader.readMemChunk(chunkMetadata);
          ChunkReader chunkReader = new ChunkReader(chunk);

          final PageHeader pageHeader =
              PageHeader.deserializeFrom(chunk.getData(), chunkMetadata.getStatistics());
          final ByteBuffer pageBuffer = chunkReader.readPageDataWithoutUncompressing(pageHeader);

          final TsBlock tsBlock = chunkReader.readPageData(pageHeader, pageBuffer);
          final TsBlock.TsBlockRowIterator tsBlockRowIterator = tsBlock.getTsBlockRowIterator();

          long sum = 0;
          int intSum = 0;
          while (tsBlockRowIterator.hasNext()) {
            final Object[] next = tsBlockRowIterator.next();
            System.out.print(next[0]);
            System.out.print("  ");
            System.out.println(next[1]);
            sum += (Integer) next[0];
            intSum += (Integer) next[0];
          }
          System.out.println(sum);
          System.out.println(intSum);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private TsFileResource generateSingleNonAlignedSeriesFile(
      String device,
      String measurement,
      TimeRange[] chunkTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType,
      boolean isSeq)
      throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResource(isSeq);
    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    writer1.startChunkGroup(device);
    writer1.generateSimpleNonAlignedSeriesToCurrentDevice(
        measurement, chunkTimeRanges, encoding, compressionType);
    writer1.endChunkGroup();
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }
}
