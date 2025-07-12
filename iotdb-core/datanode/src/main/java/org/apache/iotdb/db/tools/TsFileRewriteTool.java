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

package org.apache.iotdb.db.tools;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TsFileRewriteTool {
  private static final AtomicInteger version = new AtomicInteger(0);
  private static final int MAX_THREADS = 4;

  public TsFileRewriteTool() {}

  private static final ThreadFactory compactionThreadFactory =
      new ThreadFactory() {
        private final AtomicInteger threadCount = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
          int threadId = threadCount.getAndIncrement();
          Thread thread = new Thread(r);
          thread.setName("pool-1-IoTDB-Compaction-Worker-" + threadId);
          return thread;
        }
      };

  public static void main(String[] args) throws InterruptedException {
    ExecutorService executor =
        Executors.newFixedThreadPool(
            args.length >= 3 ? Integer.parseInt(args[0]) : MAX_THREADS, compactionThreadFactory);
    CompletionService<File> completionService = new ExecutorCompletionService<>(executor);
    CompactionTaskManager.getInstance().start();
    CompactionTaskManager.getInstance().setWriteMergeRate(1000.0);

    if (args.length < 2) {
      System.err.println("Usage: TsFileRewriteTool <source_dir> <target_dir>");
      System.exit(1);
    }

    String source_dir = args[0];
    String target_dir = args[1];
    File source = new File(source_dir);
    File target = new File(target_dir);

    if (!target.exists() && !target.mkdirs()) {
      System.err.println("Failed to create target directory: " + target.getAbsolutePath());
      System.exit(1);
    }

    List<File> tsFiles = collectTsFiles(source);
    System.out.println("Found " + tsFiles.size() + " files to process");

    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger failureCount = new AtomicInteger(0);

    for (File tsFile : tsFiles) {
      completionService.submit(
          () -> {
            try {
              File result = compaction(tsFile, target);
              System.out.println("Processed: " + tsFile.getName() + " -> " + result.getName());
              successCount.incrementAndGet();
              return result;
            } catch (Exception e) {
              System.err.println("Error processing " + tsFile.getAbsolutePath());
              e.printStackTrace();
              failureCount.incrementAndGet();
              throw e;
            }
          });
    }

    for (int i = 0; i < tsFiles.size(); i++) {
      try {
        completionService.take().get();
      } catch (ExecutionException e) {
      }
    }

    System.out.println("\nProcessing completed!");
    System.out.println("Success: " + successCount.get());
    System.out.println("Failed: " + failureCount.get());

    executor.shutdown();
    try {
      if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }

    CompactionTaskManager.getInstance().stop();
  }

  private static List<File> collectTsFiles(File sourceDir) {
    List<File> tsFiles = new ArrayList<>();
    collectTsFilesRecursive(sourceDir, tsFiles);
    return tsFiles;
  }

  private static void collectTsFilesRecursive(File dir, List<File> result) {
    File[] files = dir.listFiles();
    if (files == null) return;

    for (File file : files) {
      if (file.isDirectory()) {
        collectTsFilesRecursive(file, result);
      } else if (file.isFile() && file.getName().endsWith(".tsfile")) {
        File resFile = new File(file.getPath() + ".resource");
        if (resFile.exists()) {
          result.add(file);
        }
      }
    }
  }

  private static File compaction(File file, File targetDir) throws Exception {
    TsFileResource resource = new TsFileResource(file);
    resource.deserialize();
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);

    String name =
        TsFileNameGenerator.generateNewTsFileName(
            System.currentTimeMillis(), version.getAndIncrement(), 0, 0);

    TsFileResource target = new TsFileResource(new File(targetDir, name));
    ReadPointCompactionPerformer performer =
        new ReadPointCompactionPerformer(
            Collections.singletonList(resource),
            Collections.emptyList(),
            Collections.singletonList(target));
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    target.serialize();
    return target.getTsFile();
  }
}
