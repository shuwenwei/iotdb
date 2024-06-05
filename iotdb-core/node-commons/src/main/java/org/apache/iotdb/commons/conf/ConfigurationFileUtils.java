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

package org.apache.iotdb.commons.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ConfigurationFileUtils {

  private static final String lockFileSuffix = ".lock";
  private static final long maxTimeMillsToAcquireLock = TimeUnit.SECONDS.toMillis(20);
  private static final long waitTimeMillsPerCheck = TimeUnit.MILLISECONDS.toMillis(100);
  private static Logger logger = LoggerFactory.getLogger(ConfigurationFileUtils.class);
  private static String license =
      "#\n"
          + "# Licensed to the Apache Software Foundation (ASF) under one\n"
          + "# or more contributor license agreements.  See the NOTICE file\n"
          + "# distributed with this work for additional information\n"
          + "# regarding copyright ownership.  The ASF licenses this file\n"
          + "# to you under the Apache License, Version 2.0 (the\n"
          + "# \"License\"); you may not use this file except in compliance\n"
          + "# with the License.  You may obtain a copy of the License at\n"
          + "#\n"
          + "#     http://www.apache.org/licenses/LICENSE-2.0\n"
          + "#\n"
          + "# Unless required by applicable law or agreed to in writing,\n"
          + "# software distributed under the License is distributed on an\n"
          + "# \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n"
          + "# KIND, either express or implied.  See the License for the\n"
          + "# specific language governing permissions and limitations\n"
          + "# under the License.";

  public static void checkAndMayUpdate(
      URL systemUrl, URL configNodeUrl, URL dataNodeUrl, URL commonUrl)
      throws IOException, InterruptedException {
    if (systemUrl == null || configNodeUrl == null || dataNodeUrl == null || commonUrl == null) {
      return;
    }
    File systemFile = new File(systemUrl.getFile());
    File configNodeFile = new File(configNodeUrl.getFile());
    File dataNodeFile = new File(dataNodeUrl.getFile());
    File commonFile = new File(commonUrl.getFile());

    if (systemFile.exists()) {
      return;
    }
    boolean canUpdate = (configNodeFile.exists() || dataNodeFile.exists()) && commonFile.exists();
    if (!canUpdate) {
      return;
    }

    File lockFile = new File(systemFile.getPath() + lockFileSuffix);
    acquireTargetFileLock(lockFile);
    try {
      // other progress updated this file
      if (systemFile.exists()) {
        return;
      }
      try (RandomAccessFile raf = new RandomAccessFile(lockFile, "rw")) {
        raf.write(license.getBytes());
        String configNodeContent = readConfigLines(configNodeFile);
        raf.write(configNodeContent.getBytes());
        String dataNodeContent = readConfigLines(dataNodeFile);
        raf.write(dataNodeContent.getBytes());
        String commonContent = readConfigLines(commonFile);
        raf.write(commonContent.getBytes());
      }
      Files.move(lockFile.toPath(), systemFile.toPath());
    } finally {
      releaseFileLock(lockFile);
    }
  }

  public static String readConfigFileContent(URL url) throws IOException {
    if (url == null) {
      return "";
    }
    File f = new File(url.getFile());
    if (!f.exists()) {
      return "";
    }
    return readConfigLines(f);
  }

  public static List<String> filterImmutableConfigItems(Properties properties) {
    List<String> ignoredConfigItems = new ArrayList<>();
    return ignoredConfigItems;
  }

  public static void updateConfigurationFile(File file, Properties newConfigItems)
      throws IOException, InterruptedException {
    try {
      throw new RuntimeException("");
    } catch (Exception e) {
      logger.error("update configuration file", e);
    }
    File lockFile = new File(file.getPath() + lockFileSuffix);
    acquireTargetFileLock(lockFile);
    try {
      List<String> lines = new ArrayList<>();
      try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
        String line = null;
        while ((line = reader.readLine()) != null) {
          lines.add(line);
        }
      }
      try (FileWriter writer = new FileWriter(lockFile)) {
        for (String currentLine : lines) {
          if (currentLine.trim().isEmpty() || currentLine.trim().startsWith("#")) {
            writer.write(currentLine + System.lineSeparator());
            continue;
          }
          int equalsIndex = currentLine.indexOf('=');
          // replace old config
          if (equalsIndex != -1) {
            String key = currentLine.substring(0, equalsIndex).trim();
            if (newConfigItems.containsKey(key)) {
              writer.write(key + "=" + newConfigItems.remove(key) + System.lineSeparator());
            } else {
              writer.write(currentLine + System.lineSeparator());
            }
          }
        }
        // add new config items
        newConfigItems.store(writer, null);
      }
      Files.move(lockFile.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
    } finally {
      releaseFileLock(lockFile);
    }
  }

  private static Properties loadProperties(File file) throws IOException {
    Properties properties = new Properties();
    try (FileReader reader = new FileReader(file)) {
      properties.load(reader);
    }
    return properties;
  }

  private static String readConfigLines(File file) throws IOException {
    if (!file.exists()) {
      return "";
    }
    byte[] bytes = Files.readAllBytes(file.toPath());
    String content = new String(bytes);
    return content.replace(license, "");
  }

  private static void acquireTargetFileLock(File file) throws IOException, InterruptedException {
    long totalWaitTime = 0;
    while (totalWaitTime < maxTimeMillsToAcquireLock) {
      if (file.createNewFile()) {
        return;
      }
      totalWaitTime += waitTimeMillsPerCheck;
      Thread.sleep(waitTimeMillsPerCheck);
    }
    logger.warn(
        "Waiting for {} seconds to acquire configuration file update lock."
            + " There may have been an unexpected interruption in the last"
            + " configuration file update. Ignore temporary file {}",
        totalWaitTime / 1000,
        file.getName());
  }

  private static void releaseFileLock(File file) throws IOException {
    Files.deleteIfExists(file.toPath());
  }
}
