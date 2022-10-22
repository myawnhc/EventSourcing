package org.hazelcast.eventsourcing.dynamicconfig;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;

import java.io.Serializable;
import java.util.List;
import java.util.logging.Logger;

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