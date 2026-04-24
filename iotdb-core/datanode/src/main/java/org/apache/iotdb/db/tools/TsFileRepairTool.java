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
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.file.SealedTsFileRecoverPerformer;
import org.apache.iotdb.db.utils.EncryptDBUtils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collections;

public class TsFileRepairTool {
  private static final String REPAIR_FILE_SUFFIX = ".repair";
  private static final String REWRITE_FILE_SUFFIX = ".rewrite";
  private static final String PARTIAL_SUCCESS_MESSAGE =
      "Repair succeeded, but some damaged data was skipped.";

  private enum RepairStrategy {
    NO_REPAIR,
    SEALED_RECOVER,
    REGENERATE_METADATA,
    REWRITE
  }

  private static class FileInspectionResult {
    private final boolean complete;
    private final boolean resourceFileExists;
    private final boolean dataReadableAndCorrect;
    private final boolean metadataReadable;
    private final RepairStrategy repairStrategy;

    private FileInspectionResult(
        boolean complete,
        boolean resourceFileExists,
        boolean dataReadableAndCorrect,
        boolean metadataReadable,
        RepairStrategy repairStrategy) {
      this.complete = complete;
      this.resourceFileExists = resourceFileExists;
      this.dataReadableAndCorrect = dataReadableAndCorrect;
      this.metadataReadable = metadataReadable;
      this.repairStrategy = repairStrategy;
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("Usage: TsFileRepairTool <tsfile-path>");
    }

    TsFileRepairTool repairTool = new TsFileRepairTool();
    CompactionTaskManager.getInstance().start();
    try {
      repairTool.repair(new TsFileResource(new File(args[0])));
    } finally {
      CompactionTaskManager.getInstance().stop();
    }
  }

  public void repair(TsFileResource resource) throws Exception {
    if (!resource.getTsFile().exists()) {
      throw new IOException("TsFile does not exist: " + resource.getTsFilePath());
    }

    printInfo("Source file: " + resource.getTsFilePath());
    printAction("Inspect tsfile before repair.");
    FileInspectionResult inspectionResult = inspectFile(resource);
    printInspectionResult(inspectionResult);

    if (inspectionResult.repairStrategy == RepairStrategy.NO_REPAIR) {
      printSuccess("Source tsfile is already correct. No repair is needed.");
      return;
    }
    TsFileResource targetResource =
        inspectionResult.repairStrategy == RepairStrategy.REWRITE
            ? prepareEmptyTargetResource(resource)
            : prepareTargetResource(resource);
    printInfo("Repair target file: " + targetResource.getTsFilePath());
    executeRepair(resource, targetResource, inspectionResult);
  }

  private boolean isComplete(TsFileResource resource) throws IOException {
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(
            resource.getTsFilePath(),
            EncryptDBUtils.getFirstEncryptParamFromTSFilePath(resource.getTsFilePath()))) {
      return reader.isComplete();
    }
  }

  private boolean canReadMetadata(TsFileResource resource) {
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(
            resource.getTsFilePath(),
            EncryptDBUtils.getFirstEncryptParamFromTSFilePath(resource.getTsFilePath()))) {
      reader.getAllTimeseriesMetadata(true);
      return true;
    } catch (Exception | OutOfMemoryError e) {
      return false;
    }
  }

  private boolean isDataReadableAndCorrect(TsFileResource resource) {
    try {
      return TsFileResourceUtils.validateTsFileDataCorrectness(resource);
    } catch (OutOfMemoryError e) {
      return false;
    }
  }

  private FileInspectionResult inspectFile(TsFileResource resource) throws IOException {
    boolean complete = isComplete(resource);
    boolean resourceFileExists = resource.resourceFileExists();
    boolean dataReadableAndCorrect = false;
    boolean metadataReadable = false;
    if (complete) {
      dataReadableAndCorrect = isDataReadableAndCorrect(resource);
      if (dataReadableAndCorrect) {
        metadataReadable = true;
      } else {
        metadataReadable = canReadMetadata(resource);
      }
    }
    return new FileInspectionResult(
        complete,
        resourceFileExists,
        dataReadableAndCorrect,
        metadataReadable,
        chooseRepairStrategy(
            complete, resourceFileExists, dataReadableAndCorrect, metadataReadable));
  }

  private RepairStrategy chooseRepairStrategy(
      boolean complete,
      boolean resourceFileExists,
      boolean dataReadableAndCorrect,
      boolean metadataReadable) {
    if (dataReadableAndCorrect) {
      return RepairStrategy.NO_REPAIR;
    }
    if (metadataReadable) {
      return RepairStrategy.REWRITE;
    }
    if (!complete || !resourceFileExists) {
      return RepairStrategy.SEALED_RECOVER;
    }
    return RepairStrategy.REGENERATE_METADATA;
  }

  private void executeRepair(
      TsFileResource sourceResource,
      TsFileResource targetResource,
      FileInspectionResult inspectionResult)
      throws Exception {
    switch (inspectionResult.repairStrategy) {
      case SEALED_RECOVER:
        repairBySealedRecover(targetResource, inspectionResult);
        return;
      case REGENERATE_METADATA:
        repairByRegeneratingMetadata(targetResource);
        return;
      case REWRITE:
        repairByRewrite(sourceResource, targetResource);
        return;
      case NO_REPAIR:
      default:
        break;
    }
  }

  private void repairBySealedRecover(TsFileResource resource, FileInspectionResult inspectionResult)
      throws Exception {
    printAction("Recover sealed file directly.");
    recoverSealedFile(resource, false);
    if (isDataReadableAndCorrect(resource)) {
      printRepairResult(resource, "Repair succeeded by sealed file recovery.");
      return;
    }
    printProblem("TsFile data validation failed after sealed file recovery.");
    if (inspectionResult.metadataReadable) {
      printAction("Rewrite readable data to a repaired file.");
      rewriteBrokenButReadableFile(resource, resource);
      return;
    }
    printProblem("TsFile metadata is unreadable.");
    repairByRegeneratingMetadata(resource);
  }

  private void repairByRegeneratingMetadata(TsFileResource resource) throws Exception {
    printAction("Break file tail and regenerate metadata/resource file.");
    breakFileTail(resource);
    recoverSealedFile(resource, true);
    if (isDataReadableAndCorrect(resource)) {
      printRepairResult(resource, "Repair succeeded after regenerating metadata.");
      return;
    }
    printProblem("TsFile data validation still failed after metadata regeneration.");
    printAction("Rewrite readable data to a repaired file.");
    rewriteBrokenButReadableFile(resource, resource);
  }

  private void repairByRewrite(TsFileResource sourceResource, TsFileResource targetResource)
      throws Exception {
    printAction("Rewrite readable data to a repaired file.");
    rewriteBrokenButReadableFile(sourceResource, targetResource);
  }

  private void recoverSealedFile(TsFileResource resource, boolean forceRegenerateResource)
      throws Exception {
    if (forceRegenerateResource) {
      resource.removeResourceFile();
    }
    try (SealedTsFileRecoverPerformer performer = new SealedTsFileRecoverPerformer(resource)) {
      performer.recover();
    }
  }

  private void breakFileTail(TsFileResource resource) throws IOException {
    long fileSize = resource.getTsFile().length();
    long minTsFileSize = TSFileConfig.MAGIC_STRING.getBytes().length * 2L + Byte.BYTES;
    if (fileSize <= minTsFileSize) {
      throw new IOException("TsFile is too small to repair by truncating tail: " + fileSize);
    }
    try (FileChannel channel =
        FileChannel.open(resource.getTsFile().toPath(), StandardOpenOption.WRITE)) {
      channel.truncate(fileSize - 1);
    }
  }

  private void rewriteBrokenButReadableFile(
      TsFileResource sourceResource, TsFileResource finalTargetResource) throws Exception {
    prepareResourceForRewrite(sourceResource);
    boolean needTemporaryRewriteTarget =
        sourceResource.getTsFilePath().equals(finalTargetResource.getTsFilePath());
    TsFileResource rewriteTarget =
        needTemporaryRewriteTarget
            ? new TsFileResource(generateRewriteTargetFile(finalTargetResource.getTsFile()))
            : finalTargetResource;
    if (needTemporaryRewriteTarget) {
      cleanupTargetResource(rewriteTarget);
    }
    ReadPointCompactionPerformer performer =
        new ReadPointCompactionPerformer(
            Collections.singletonList(sourceResource),
            Collections.emptyList(),
            Collections.singletonList(rewriteTarget));
    performer.setSummary(new CompactionTaskSummary());
    String originalThreadName = Thread.currentThread().getName();
    try {
      Thread.currentThread().setName("pool-1-IoTDB-Compaction-Worker-1");
      performer.perform();
    } finally {
      Thread.currentThread().setName(originalThreadName);
    }
    ensureResourceFileGenerated(rewriteTarget);
    ensureRewriteTargetGenerated(rewriteTarget);
    if (needTemporaryRewriteTarget) {
      replaceWithRewriteResult(finalTargetResource, rewriteTarget);
    }
    validateRewriteResult(finalTargetResource);
  }

  private void prepareResourceForRewrite(TsFileResource resource) throws Exception {
    if (!resource.resourceFileExists()) {
      printAction("Regenerate resource file for rewrite.");
      resource.setTimeIndex(new FileTimeIndex(Long.MIN_VALUE, Long.MAX_VALUE));
    } else {
      resource.deserialize();
    }
  }

  private TsFileResource prepareTargetResource(TsFileResource sourceResource) throws IOException {
    TsFileResource targetResource = prepareEmptyTargetResource(sourceResource);
    Files.copy(
        sourceResource.getTsFile().toPath(),
        targetResource.getTsFile().toPath(),
        StandardCopyOption.REPLACE_EXISTING,
        StandardCopyOption.COPY_ATTRIBUTES);
    copySideFileIfExists(
        new File(sourceResource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX),
        new File(targetResource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX));
    copySideFileIfExists(
        new File(sourceResource.getTsFilePath() + ModificationFile.FILE_SUFFIX),
        new File(targetResource.getTsFilePath() + ModificationFile.FILE_SUFFIX));
    copySideFileIfExists(
        new File(sourceResource.getTsFilePath() + ModificationFileV1.FILE_SUFFIX),
        new File(targetResource.getTsFilePath() + ModificationFileV1.FILE_SUFFIX));
    return targetResource;
  }

  private TsFileResource prepareEmptyTargetResource(TsFileResource sourceResource)
      throws IOException {
    TsFileResource targetResource =
        new TsFileResource(generateTargetFile(sourceResource.getTsFile()));
    cleanupTargetResource(targetResource);
    return targetResource;
  }

  private File generateTargetFile(File sourceFile) {
    String sourceName = sourceFile.getName();
    String targetName =
        sourceName.endsWith(TsFileConstant.TSFILE_SUFFIX)
            ? sourceName.replace(
                TsFileConstant.TSFILE_SUFFIX, REPAIR_FILE_SUFFIX + TsFileConstant.TSFILE_SUFFIX)
            : sourceName + REPAIR_FILE_SUFFIX;
    return new File(sourceFile.getParentFile(), targetName);
  }

  private File generateRewriteTargetFile(File targetFile) {
    String targetName = targetFile.getName();
    String rewriteTargetName =
        targetName.endsWith(TsFileConstant.TSFILE_SUFFIX)
            ? targetName.replace(
                TsFileConstant.TSFILE_SUFFIX, REWRITE_FILE_SUFFIX + TsFileConstant.TSFILE_SUFFIX)
            : targetName + REWRITE_FILE_SUFFIX;
    return new File(targetFile.getParentFile(), rewriteTargetName);
  }

  private void cleanupTargetResource(TsFileResource resource) throws IOException {
    Files.deleteIfExists(resource.getTsFile().toPath());
    Files.deleteIfExists(
        new File(resource.getTsFilePath() + TsFileIOWriter.CHUNK_METADATA_TEMP_FILE_SUFFIX)
            .toPath());
    Files.deleteIfExists(
        new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).toPath());
    Files.deleteIfExists(
        new File(
                resource.getTsFilePath()
                    + TsFileResource.RESOURCE_SUFFIX
                    + TsFileResource.TEMP_SUFFIX)
            .toPath());
    Files.deleteIfExists(
        new File(resource.getTsFilePath() + ModificationFile.FILE_SUFFIX).toPath());
    Files.deleteIfExists(
        new File(resource.getTsFilePath() + ModificationFile.COMPACTION_FILE_SUFFIX).toPath());
    Files.deleteIfExists(
        new File(resource.getTsFilePath() + ModificationFileV1.FILE_SUFFIX).toPath());
    Files.deleteIfExists(
        new File(resource.getTsFilePath() + ModificationFileV1.COMPACTION_FILE_SUFFIX).toPath());
  }

  private void replaceWithRewriteResult(TsFileResource targetResource, TsFileResource rewriteTarget)
      throws IOException {
    cleanupTargetResource(targetResource);
    Files.move(
        rewriteTarget.getTsFile().toPath(),
        targetResource.getTsFile().toPath(),
        StandardCopyOption.REPLACE_EXISTING);
    Files.move(
        new File(rewriteTarget.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).toPath(),
        new File(targetResource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).toPath(),
        StandardCopyOption.REPLACE_EXISTING);
    moveSideFileIfExists(
        new File(rewriteTarget.getTsFilePath() + ModificationFile.FILE_SUFFIX),
        new File(targetResource.getTsFilePath() + ModificationFile.FILE_SUFFIX));
    moveSideFileIfExists(
        new File(rewriteTarget.getTsFilePath() + ModificationFileV1.FILE_SUFFIX),
        new File(targetResource.getTsFilePath() + ModificationFileV1.FILE_SUFFIX));
    moveSideFileIfExists(
        new File(rewriteTarget.getTsFilePath() + ModificationFile.COMPACTION_FILE_SUFFIX),
        new File(targetResource.getTsFilePath() + ModificationFile.COMPACTION_FILE_SUFFIX));
    moveSideFileIfExists(
        new File(rewriteTarget.getTsFilePath() + ModificationFileV1.COMPACTION_FILE_SUFFIX),
        new File(targetResource.getTsFilePath() + ModificationFileV1.COMPACTION_FILE_SUFFIX));
    cleanupTargetResource(rewriteTarget);
  }

  private void ensureRewriteTargetGenerated(TsFileResource rewriteTarget) throws IOException {
    if (!rewriteTarget.getTsFile().exists()) {
      throw new IOException(
          "Rewrite target file is not generated: " + rewriteTarget.getTsFilePath());
    }
    if (!new File(rewriteTarget.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists()) {
      throw new IOException(
          "Rewrite target resource file is not generated: "
              + rewriteTarget.getTsFilePath()
              + TsFileResource.RESOURCE_SUFFIX);
    }
  }

  private void ensureRewriteProducedUsableResult(
      TsFileResource rewriteTarget, java.util.List<String> skippedData) throws IOException {
    if (rewriteTarget.getTsFile().exists()) {
      return;
    }
    printSkippedData(skippedData);
    if (!skippedData.isEmpty()) {
      throw new IOException(
          "Rewrite skipped all readable data and produced no repaired file: "
              + rewriteTarget.getTsFilePath());
    }
    throw new IOException(
        "Rewrite finished but produced no repaired file: " + rewriteTarget.getTsFilePath());
  }

  private void ensureResourceFileGenerated(TsFileResource resource) throws IOException {
    if (!resource.resourceFileExists()) {
      resource.serialize();
    }
  }

  private void validateRewriteResult(TsFileResource targetResource) throws IOException {
    if (!isDataReadableAndCorrect(targetResource)) {
      throw new IOException("Rewritten file validation failed: " + targetResource.getTsFilePath());
    }
  }

  private void copySideFileIfExists(File sourceFile, File targetFile) throws IOException {
    if (!sourceFile.exists()) {
      return;
    }
    Files.copy(
        sourceFile.toPath(),
        targetFile.toPath(),
        StandardCopyOption.REPLACE_EXISTING,
        StandardCopyOption.COPY_ATTRIBUTES);
  }

  private void moveSideFileIfExists(File sourceFile, File targetFile) throws IOException {
    if (!sourceFile.exists()) {
      return;
    }
    Files.move(sourceFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
  }

  private void printInspectionResult(FileInspectionResult inspectionResult) {
    if (inspectionResult.complete) {
      printInfo("TsFile is complete.");
    } else {
      printProblem("TsFile is incomplete.");
    }
    if (inspectionResult.resourceFileExists) {
      printInfo("Resource file exists.");
    } else {
      printProblem("Resource file is missing.");
    }
    if (inspectionResult.dataReadableAndCorrect) {
      printInfo("TsFile data validation passed.");
    } else {
      printProblem("TsFile data validation failed.");
    }
    if (inspectionResult.metadataReadable) {
      printInfo("TsFile metadata is readable.");
    } else {
      printProblem("TsFile metadata is unreadable.");
    }
    printInfo("Selected repair strategy: " + inspectionResult.repairStrategy.name());
  }

  private void printInfo(String message) {
    System.out.println("[TsFileRepairTool][INFO] " + message);
  }

  private void printProblem(String message) {
    System.out.println("[TsFileRepairTool][PROBLEM] " + message);
  }

  private void printAction(String message) {
    System.out.println("[TsFileRepairTool][ACTION] " + message);
  }

  private void printSuccess(String message) {
    System.out.println("[TsFileRepairTool][SUCCESS] " + message);
  }

  private void printRepairResult(TsFileResource targetResource, String message) {
    printSuccess(message);
    printInfo("Output file: " + targetResource.getTsFilePath());
  }

  private void printSkippedData(java.util.List<String> skippedData) {
    if (skippedData.isEmpty()) {
      return;
    }
    printProblem("Repair skipped some damaged data.");
    for (String skippedItem : skippedData) {
      printProblem(skippedItem);
    }
  }
}
