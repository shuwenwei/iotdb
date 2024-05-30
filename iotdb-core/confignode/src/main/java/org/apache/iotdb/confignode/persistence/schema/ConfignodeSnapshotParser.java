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
package org.apache.iotdb.confignode.persistence.schema;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.persistence.TTLInfo;

import org.apache.tsfile.utils.Pair;
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

public class ConfignodeSnapshotParser {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfignodeSnapshotParser.class);
  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  private static final String SNAPSHOT_CLUSTER_SCHEMA_FILENAME = "cluster_schema.bin";

  private static final String SNAPSHOT_TEMPLATE_FILENAME = "template_info.bin";

  private ConfignodeSnapshotParser() {
    // Empty constructor
  }

  private static Path getLatestSnapshotPath(List<Path> snapshotPathList) {
    if (snapshotPathList.isEmpty()) {
      return null;
    }
    final Path[] pathArray = snapshotPathList.toArray(new Path[0]);
    Arrays.sort(
        pathArray,
        (o1, o2) -> {
          final String index1 = o1.toFile().getName().split("_")[1];
          final String index2 = o2.toFile().getName().split("_")[1];
          return Long.compare(Long.parseLong(index2), Long.parseLong(index1));
        });
    return pathArray[0];
  }

  public static List<Pair<Pair<Path, Path>, CNSnapshotFileType>> getSnapshots() throws IOException {
    final List<Pair<Pair<Path, Path>, CNSnapshotFileType>> snapshotPairList = new ArrayList<>();
    final String snapshotPath = CONF.getConsensusDir();
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(Paths.get(snapshotPath), "[0-9]*-[0-9]*-[0-9]*-[0-9]*-[0-9]*")) {
      // In confignode there is only one consensus dir.
      // Get into confignode consensus dir
      for (Path path : stream) {
        try (DirectoryStream<Path> filestream =
            Files.newDirectoryStream(Paths.get(path.toString() + File.separator + "sm"))) {
          ArrayList<Path> snapshotList = new ArrayList<>();
          for (Path snapshotFolder : filestream) {
            if (snapshotFolder.toFile().isDirectory()) {
              snapshotList.add(snapshotFolder);
            }
          }
          final Path latestSnapshotPath = getLatestSnapshotPath(snapshotList);

          if (latestSnapshotPath != null) {
            // Get role files.
            final String rolePath =
                latestSnapshotPath
                    + File.separator
                    + IoTDBConstant.SYSTEM_FOLDER_NAME
                    + File.separator
                    + "roles";
            try (DirectoryStream<Path> roleStream = Files.newDirectoryStream(Paths.get(rolePath))) {
              for (Path role : roleStream) {
                Pair<Path, Path> roleFile = new Pair<>(role, null);
                snapshotPairList.add(new Pair<>(roleFile, CNSnapshotFileType.ROLE));
              }
            }
            // Get user files.
            final String userPath =
                latestSnapshotPath
                    + File.separator
                    + IoTDBConstant.SYSTEM_FOLDER_NAME
                    + File.separator
                    + "users";
            try (DirectoryStream<Path> userStream = Files.newDirectoryStream(Paths.get(userPath))) {
              final List<Path> userFilePath = new ArrayList<>();
              final List<Path> userRoleFilePath = new ArrayList<>();
              for (Path user : userStream) {
                if (user.getFileName().toString().contains("_role.profile")) {
                  userRoleFilePath.add(user);
                } else {
                  userFilePath.add(user);
                }
              }
              // We should add user file firstly.
              for (Path user : userFilePath) {
                snapshotPairList.add(new Pair<>(new Pair<>(user, null), CNSnapshotFileType.USER));
              }
              for (Path roleList : userRoleFilePath) {
                snapshotPairList.add(
                    new Pair<>(new Pair<>(roleList, null), CNSnapshotFileType.USER_ROLE));
              }
            }

            // Get cluster schema info file and template file.
            final File schemaInfoFile =
                SystemFileFactory.INSTANCE.getFile(
                    latestSnapshotPath + File.separator + SNAPSHOT_CLUSTER_SCHEMA_FILENAME);
            final File templateInfoFile =
                SystemFileFactory.INSTANCE.getFile(
                    latestSnapshotPath + File.separator + SNAPSHOT_TEMPLATE_FILENAME);
            if (schemaInfoFile.exists() && templateInfoFile.exists()) {
              snapshotPairList.add(
                  new Pair<>(
                      new Pair<>(schemaInfoFile.toPath(), templateInfoFile.toPath()),
                      CNSnapshotFileType.SCHEMA));
            }
            // Get ttl info file
            final File ttlInfoFile =
                SystemFileFactory.INSTANCE.getFile(
                    latestSnapshotPath + File.separator + TTLInfo.SNAPSHOT_FILENAME);
            if (ttlInfoFile.exists()) {
              snapshotPairList.add(
                  new Pair<>(new Pair<>(ttlInfoFile.toPath(), null), CNSnapshotFileType.TTL));
            }
          }
        }
      }
    }
    return snapshotPairList;
  }

  public static CNPhysicalPlanGenerator translate2PhysicalPlan(
      Path path1, Path path2, CNSnapshotFileType type) throws IOException {
    if (type == CNSnapshotFileType.SCHEMA && (path1 == null || path2 == null)) {
      LOGGER.warn("Schema_template require schema info file and template file");
      return null;
    } else if (path1 == null) {
      LOGGER.warn("Path1 should not be null");
      return null;
    }

    if (!path1.toFile().exists()) {
      LOGGER.warn("File {} not exists", path1.toFile().getName());
      return null;
    }

    if (type == CNSnapshotFileType.SCHEMA) {
      return new CNPhysicalPlanGenerator(path1, path2);
    } else {
      return new CNPhysicalPlanGenerator(path1, type);
    }
  }
}
