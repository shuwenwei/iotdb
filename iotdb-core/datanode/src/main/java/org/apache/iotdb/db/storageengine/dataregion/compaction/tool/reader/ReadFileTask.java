package org.apache.iotdb.db.storageengine.dataregion.compaction.tool.reader;

import org.apache.iotdb.db.storageengine.dataregion.compaction.tool.TsFileStatisticReader;

import java.util.List;
import java.util.concurrent.Callable;

public class ReadFileTask implements Callable<List<TsFileStatisticReader.ChunkGroupStatistics>> {

  private String filePath;

  public ReadFileTask(String filePath) {
    this.filePath = filePath;
  }

  @Override
  public List<TsFileStatisticReader.ChunkGroupStatistics> call() throws Exception {
    try (TsFileStatisticReader reader = new TsFileStatisticReader(filePath)) {
      return reader.getChunkGroupStatistics();
    }
  }
}
