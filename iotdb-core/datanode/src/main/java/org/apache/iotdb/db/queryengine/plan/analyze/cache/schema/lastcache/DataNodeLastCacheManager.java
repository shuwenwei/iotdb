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

package org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.lastcache;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.SchemaCacheEntry;
import org.apache.iotdb.tsfile.read.TimeValuePair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataNodeLastCacheManager {
  private static final Logger logger = LoggerFactory.getLogger(DataNodeLastCacheManager.class);

  private static final boolean CACHE_ENABLED =
      CommonDescriptor.getInstance().getConfig().isLastCacheEnable();

  /**
   * get the last cache value from time series
   *
   * @param entry schema cache entry in DataNodeSchemaCache
   * @return the last cache value
   */
  public static TimeValuePair getLastCache(SchemaCacheEntry entry) {
    if (!CACHE_ENABLED || null == entry) {
      return null;
    }
    ILastCacheContainer lastCacheContainer = entry.getLastCacheContainer();
    return lastCacheContainer == null ? null : lastCacheContainer.getCachedLast();
  }

  /**
   * update the last cache value of time series
   *
   * @param entry schema cache entry in DataNodeSchemaCache
   * @param timeValuePair the latest point value
   * @param highPriorityUpdate the last value from insertPlan is high priority
   * @param latestFlushedTime latest flushed time
   * @return increasing of memory usage
   */
  public static int updateLastCache(
      SchemaCacheEntry entry,
      TimeValuePair timeValuePair,
      boolean highPriorityUpdate,
      Long latestFlushedTime) {
    if (!CACHE_ENABLED || null == entry) {
      return 0;
    }
    return entry.updateLastCache(timeValuePair, highPriorityUpdate, latestFlushedTime);
  }

  public static int invalidateLastCache(SchemaCacheEntry entry) {
    if (!CACHE_ENABLED || null == entry) {
      return 0;
    }
    return entry.invalidateLastCache();
  }
}
