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
package org.apache.iotdb.db.tools.schema;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SchemaRegionSnapshotParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegionSnapshotParser.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private SchemaRegionSnapshotParser() {
    // empty constructor
  }

  private static Path getLatestSnapshotPath(List<Path> snapshotPathList) {
    if (snapshotPathList.isEmpty()) {
      return null;
    }
    Path[] pathArray = snapshotPathList.toArray(new Path[0]);
    Arrays.sort(
        pathArray,
        (o1, o2) -> {
          String index1 = o1.toFile().getName().split("_")[1];
          String index2 = o2.toFile().getName().split("_")[2];
          return Long.compare(Long.parseLong(index1), Long.parseLong(index2));
        });
    return pathArray[0];
  }

  // return all schema region's latest snapshot units in this datanode.
  public static List<Pair<Path, Path>> getSnapshotPaths() {
    String snapshotPath = CONFIG.getSchemaRegionConsensusDir();
    File snapshotDir = new File(snapshotPath);
    ArrayList<Pair<Path, Path>> snapshotUnits = new ArrayList<>();

    // get schema regin path
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(snapshotDir.toPath(), "[0-9]*-[0-9]*-[0-9]*-[0-9]*-[0-9]*")) {
      for (Path path : stream) {
        try (DirectoryStream<Path> filestream =
            Files.newDirectoryStream(Paths.get(path.toString() + File.separator + "sm"))) {
          // find the latest snapshots
          ArrayList<Path> snapshotList = new ArrayList<>();
          for (Path snapshotFolder : filestream) {
            if (snapshotFolder.toFile().isDirectory()) {
              snapshotList.add(snapshotFolder);
            }
          }
          Path latestSnapshotPath = getLatestSnapshotPath(snapshotList);
          if (latestSnapshotPath != null) {
            // get metadata from the latest snapshot folder.
            File mtreeSnapshot =
                SystemFileFactory.INSTANCE.getFile(
                    latestSnapshotPath + File.separator + SchemaConstant.MTREE_SNAPSHOT);
            File tagSnapshot =
                SystemFileFactory.INSTANCE.getFile(
                    latestSnapshotPath + File.separator + SchemaConstant.TAG_LOG_SNAPSHOT);
            if (mtreeSnapshot.exists()) {
              snapshotUnits.add(
                  new Pair<>(
                      mtreeSnapshot.toPath(), tagSnapshot.exists() ? tagSnapshot.toPath() : null));
            }
          }
        }
      }
    } catch (IOException exception) {
      LOGGER.warn("cannot construct snapshot directory stream", exception);
    }
    return snapshotUnits;
  }

  // in schema snapshot path: datanode/consensus/schema_region/47474747-4747-4747-4747-000200000000
  // this func will get schema region id = 47474747-4747-4747-4747-000200000000's latest snapshot.
  // In one schema region, there is only one snapshot unit.
  public static Pair<Path, Path> getSnapshotPaths(String schemaRegionId) {
    String snapshotPath = CONFIG.getSchemaRegionConsensusDir();
    File snapshotDir =
        new File(snapshotPath + File.separator + schemaRegionId + File.separator + "sm");

    // get the latest snapshot file
    ArrayList<Path> snapshotList = new ArrayList<>();
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(snapshotDir.toPath(), "[0-9]*_[0-9]*")) {
      for (Path path : stream) {
        snapshotList.add(path);
      }
    } catch (IOException ioException) {
      LOGGER.warn("ioexception when get {}'s folder", schemaRegionId, ioException);
      return null;
    }
    Path latestSnapshotPath = getLatestSnapshotPath(snapshotList);
    if (latestSnapshotPath != null) {
      // get metadata from the latest snapshot folder.
      File mtreeSnapshot =
          SystemFileFactory.INSTANCE.getFile(
              latestSnapshotPath + File.separator + SchemaConstant.MTREE_SNAPSHOT);
      File tagSnapshot =
          SystemFileFactory.INSTANCE.getFile(
              latestSnapshotPath + File.separator + SchemaConstant.TAG_LOG_SNAPSHOT);
      if (mtreeSnapshot.exists()) {
        return new Pair<>(
            mtreeSnapshot.toPath(), tagSnapshot.exists() ? tagSnapshot.toPath() : null);
      }
    }
    return null;
  }

  public static SRStatementGenerator translate2Statements(
      Path mtreePath, Path tagFilePath, PartialPath databasePath) throws IOException {
    if (mtreePath == null) {
      return null;
    }
    File mtreefile = mtreePath.toFile();
    File tagfile;
    if (tagFilePath != null && tagFilePath.toFile().exists()) {
      tagfile = tagFilePath.toFile();
    } else {
      tagfile = null;
    }

    if (!mtreefile.exists()) {
      return null;
    }

    if (!mtreefile.getName().equals(SchemaConstant.MTREE_SNAPSHOT)) {
      throw new IllegalArgumentException(
          String.format(
              "%s is not allowed, only support %s",
              mtreefile.getName(), SchemaConstant.MTREE_SNAPSHOT));
    }
    if (tagfile != null && !tagfile.getName().equals(SchemaConstant.TAG_LOG_SNAPSHOT)) {
      throw new IllegalArgumentException(
          String.format(
              " %s is not allowed, only support %s",
              tagfile.getName(), SchemaConstant.TAG_LOG_SNAPSHOT));
    }
    return new SRStatementGenerator(mtreefile, tagfile, databasePath);
  }
}