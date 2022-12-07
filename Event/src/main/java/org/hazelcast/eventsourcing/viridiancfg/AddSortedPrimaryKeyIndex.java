/*
 * Copyright 2022 Hazelcast, Inc
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package org.hazelcast.eventsourcing.viridiancfg;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;

import java.io.Serializable;
import java.util.List;
import java.util.logging.Logger;

/** Function that can be passed into Viridian Serverless instance to enable a sorted
 * index on the primary key; although the UI doesn't provide a toggle to select whether
 * an index should be hash or sorted, sorted is the default so this workaround is probably
 * not necessary.  Will do further testing to confirm.
 */
public class AddSortedPrimaryKeyIndex implements Runnable, Serializable, HazelcastInstanceAware {
    private transient HazelcastInstance hazelcast;
    private final String mapName;
    private static final Logger logger = Logger.getLogger(AddSortedPrimaryKeyIndex.class.getName());

    public AddSortedPrimaryKeyIndex(String mapName) {
        this.mapName = mapName;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcast = hazelcastInstance;
    }

    @Override
    public void run() {
        MapConfig jmconfig = new MapConfig();
        jmconfig.setName(mapName);
        List<IndexConfig> indexConfigs = jmconfig.getIndexConfigs();
        IndexConfig pkSortedIndex = new IndexConfig().addAttribute("__key").setType(IndexType.SORTED);
        indexConfigs.add(pkSortedIndex);
        hazelcast.getConfig().addMapConfig(jmconfig);
        logger.info("Enabled sorted key on primary index for " + jmconfig.getName());
    }
}