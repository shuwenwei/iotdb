package org.apache.iotdb.db.storageengine.dataregion.compaction.cross;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.InPlaceFastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskPriorityType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.inplace.InPlaceCrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.RewriteCrossSpaceCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.SizeTieredCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.CrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class CrossSpaceCompactionSelect2Test extends AbstractCompactionTest {

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setMinCrossCompactionUnseqFileLevel(0);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    for (TsFileResource tsFileResource : seqResources) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
    }
  }

  @Test
  public void testInnerCompactionSelectInPlaceSettleTask()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(1, 1, 1, 0, 1, 1, 1, 1, false, true, 0.3, 0);
    createFiles(1, 1, 1, 0, 1, 1, 1, 1, false, true, 0.4, 0);
    SizeTieredCompactionSelector selector =
        new SizeTieredCompactionSelector("", "", 0, true, tsFileManager);
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(2, innerSpaceCompactionTasks.size());
    Assert.assertEquals(
        CompactionTaskPriorityType.IN_PLACE_SETTLE,
        innerSpaceCompactionTasks.get(0).getCompactionTaskType());
    Assert.assertEquals(
        CompactionTaskPriorityType.IN_PLACE_SETTLE,
        innerSpaceCompactionTasks.get(1).getCompactionTaskType());
  }

  @Test
  public void testInnerCompactionSelectNormalTask()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(1, 1, 1, 0, 1, 1, 1, 1, false, true, 0.6, 0);
    createFiles(1, 1, 1, 0, 1, 1, 1, 1, false, true, 0.8, 0);
    SizeTieredCompactionSelector selector =
        new SizeTieredCompactionSelector("", "", 0, true, tsFileManager);
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(0, innerSpaceCompactionTasks.size());
  }

  @Test
  public void testOverlapRatio() throws IOException, MetadataException, WriteProcessException {
    createFiles(1, 1, 1, 0, 1, 1, 1, 1, false, true);
    createFiles(1, 1, 1, 0, 1, 1, 1, 1, false, false);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, tsFileManager);
    List<CrossCompactionTaskResource> resources =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    Assert.assertEquals(1, resources.get(0).getOverlapRatio(), 0.01);
  }

  @Test
  public void testOverlapRatio1() throws IOException, MetadataException, WriteProcessException {
    createFiles(1, 1, 1, 0, 1, 1, 1, 1, false, false);
    createFiles(1, 5, 1, 0, 1, 1, 1, 1, false, true);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, tsFileManager);
    List<CrossCompactionTaskResource> resources =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    Assert.assertEquals(0.2, resources.get(0).getOverlapRatio(), 0.01);
  }

  @Test
  public void testOverlapRatio2() throws IOException, MetadataException, WriteProcessException {
    createFiles(1, 1, 1, 0, 1, 1, 1, 1, false, false);
    createFiles(2, 5, 1, 0, 1, 1, 1, 1, false, true);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, tsFileManager);
    List<CrossCompactionTaskResource> resources =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    Assert.assertEquals(0.1, resources.get(0).getOverlapRatio(), 0.01);
  }

  @Test
  public void testOverlapRatio3() throws IOException, MetadataException, WriteProcessException {
    createFiles(2, 1, 1, 0, 1, 1, 1, 1, false, false);
    createFiles(2, 5, 1, 0, 1, 1, 1, 1, false, true);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, tsFileManager);
    List<CrossCompactionTaskResource> resources =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    Assert.assertEquals(0.1, resources.get(0).getOverlapRatio(), 0.01);
  }

  @Test
  public void testOverlapRatio4() throws IOException, MetadataException, WriteProcessException {
    createFiles(1, 2, 1, 0, 1, 1, 1, 1, false, false);
    createFiles(2, 5, 1, 0, 1, 1, 1, 1, false, true);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, tsFileManager);
    List<CrossCompactionTaskResource> resources =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    Assert.assertEquals(0.2, resources.get(0).getOverlapRatio(), 0.01);
  }

  @Test
  public void testCrossCompactionSelectInplaceTask()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(1, 1, 1, 0, 1, 1, 1, 1, false, true);
    createFiles(1, 1, 1, 0, 1, 1, 1, 1, false, false);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, tsFileManager);
    List<CrossCompactionTaskResource> crossCompactionTaskResources =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    for (CrossCompactionTaskResource taskResource : crossCompactionTaskResources) {
      AbstractCrossSpaceCompactionTask task;
      if (taskResource.isContainsLevelZeroFiles() || taskResource.getOverlapRatio() > 0.3) {
        task =
            new CrossSpaceCompactionTask(
                1,
                tsFileManager,
                taskResource.getSeqFiles(),
                taskResource.getUnseqFiles(),
                new InPlaceFastCompactionPerformer(),
                taskResource.getTotalMemoryCost(),
                tsFileManager.getNextCompactionTaskId());
      } else {
        task =
            new InPlaceCrossSpaceCompactionTask(
                1,
                tsFileManager,
                taskResource.getSeqFiles(),
                taskResource.getUnseqFiles(),
                new InPlaceFastCompactionPerformer(),
                taskResource.getTotalMemoryCost(),
                tsFileManager.getNextCompactionTaskId());
      }
      Assert.assertEquals(CompactionTaskPriorityType.NORMAL, task.getCompactionTaskType());
      Assert.assertEquals(1, taskResource.getOverlapRatio(), 0.1);
    }
    Assert.assertEquals(1, crossCompactionTaskResources.size());
  }

  @Test
  public void testCrossCompactionSelectInplaceTask2()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(1, 5, 1, 0, 1, 1, 1, 1, false, true, 1, 1);
    createFiles(1, 1, 1, 0, 1, 1, 1, 1, false, false, 1, 1);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, tsFileManager);
    List<CrossCompactionTaskResource> crossCompactionTaskResources =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    for (CrossCompactionTaskResource taskResource : crossCompactionTaskResources) {
      AbstractCrossSpaceCompactionTask task;
      if (taskResource.isContainsLevelZeroFiles() || taskResource.getOverlapRatio() > 0.3) {
        task =
            new CrossSpaceCompactionTask(
                1,
                tsFileManager,
                taskResource.getSeqFiles(),
                taskResource.getUnseqFiles(),
                new InPlaceFastCompactionPerformer(),
                taskResource.getTotalMemoryCost(),
                tsFileManager.getNextCompactionTaskId());
      } else {
        task =
            new InPlaceCrossSpaceCompactionTask(
                1,
                tsFileManager,
                taskResource.getSeqFiles(),
                taskResource.getUnseqFiles(),
                new InPlaceFastCompactionPerformer(),
                taskResource.getTotalMemoryCost(),
                tsFileManager.getNextCompactionTaskId());
      }
      Assert.assertEquals(CompactionTaskPriorityType.IN_PLACE, task.getCompactionTaskType());
      Assert.assertEquals(0.2, taskResource.getOverlapRatio(), 0.1);
    }
    Assert.assertEquals(1, crossCompactionTaskResources.size());
  }
}
