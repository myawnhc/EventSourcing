package org.hazelcast.eventsourcing.dynamicconfig;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;

import java.io.Serializable;
import java.util.logging.Logger;

/** Function that can be passed into Viridian Serverless instance to enable
 * MapJournal functionality.  This is needed for the Pending Events map so that
 * inserts into that map will flow into the EventSourcingPipeline.
 *
 * Once MapJournal can be toggled on via UI, this will no longer be required.
 *
 * This class is not needed for on-premise or self-managed cloud deployments.
 */
public class EnableMapJournal implements Runnable, Serializable, HazelcastInstanceAware {
    private transient HazelcastInstance hazelcast;
    private final String mapName;
    private static final Logger logger = Logger.getLogger(EnableMapJournal.class.getName());

    public EnableMapJournal(String mapName) {
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
        jmconfig.getEventJournalConfig().setEnabled(true).setCapacity(100000);
        hazelcast.getConfig().addMapConfig(jmconfig);
        logger.info("Enabled MapJournal for " + jmconfig.getName());
    }
}