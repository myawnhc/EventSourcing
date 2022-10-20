package org.hazelcast.eventsourcing;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.jet.Job;
import com.hazelcast.map.IMap;
import org.hazelcast.eventsourcing.dynamicconfig.EnableMapJournal;
import org.hazelcast.eventsourcing.event.DomainObject;
import org.hazelcast.eventsourcing.event.PartitionedSequenceKey;
import org.hazelcast.eventsourcing.event.SourcedEvent;
import org.hazelcast.eventsourcing.eventstore.EventStore;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/** Controller for event sourcing. Supports creation of the data structures
 * needed by the framework, and provides a handleEvent method to be called
 * by application code to process an event through the framework.
 *
 * @param <D> the Domain object which is updated by the event sequence
 * @param <K> the type of the key of the domain object
 * @param <E> the Event Object type that will be appended to the Event Store
 */

public class EventSourcingController<D extends DomainObject<K>, K extends Comparable<K>, E extends SourcedEvent<D,K>> {
    private HazelcastInstance hazelcast;
    private String domainObjectName;
    public String getDomainObjectName() { return domainObjectName; } // used to name pipeline job
    private static Logger logger = null; // assigned in static initializer

    // Sequence Generator
    private String sequenceGeneratorName;
    private IAtomicLong sequenceGenerator;
    public Long getNextSequence() {
        return sequenceGenerator.incrementAndGet();
    }

    // Event Store
    private String eventStoreName;
    /* Future - settings for compaction and spill-to-disk */
    private EventStore<D, K, E> eventStore;
    public EventStore<D, K, E> getEventStore() {
        return eventStore;
    }

    // View/DAO Config
    private String viewMapName;
    public String getViewMapName() {
        return viewMapName;
    }
    private IMap<K, DomainObject<K>> viewMap;
    public IMap<K, DomainObject<K>> getViewMap() { return viewMap; }

    // Pending Queue / Map config
    private String pendingEventsMapName;
    private IMap<PartitionedSequenceKey, SourcedEvent> pendingEventsMap;

    // Pipeline
    private Job pipelineJob;

    static {
        InputStream stream = EventSourcingController.class.getClassLoader().
                getResourceAsStream("logging.properties");
        try {
            LogManager.getLogManager().readConfiguration(stream);
            logger = Logger.getLogger(EventSourcingController.class.getName());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /////////////
    // Builder
    /////////////
    public static class EventSourcingControllerBuilder<D extends DomainObject<K>, K> {
        private EventSourcingController controller;

        public EventSourcingControllerBuilder(HazelcastInstance hazelcast, String domainObjectName) {
            this.controller = new EventSourcingController(hazelcast);
            this.controller.domainObjectName = domainObjectName;
            // Set default names
            this.controller.sequenceGeneratorName = domainObjectName + "_SEQ";
            this.controller.eventStoreName = domainObjectName + "_ES";
            this.controller.pendingEventsMapName = domainObjectName + "_PENDING";
            this.controller.viewMapName = domainObjectName + "_VIEW";
        }

        ///////////////////////
        // Sequence Generator
        ///////////////////////
        public EventSourcingControllerBuilder sequenceGeneratorName(String name) {
            controller.sequenceGeneratorName = name;
            return this;
        }

        private void buildSequenceGenerator() {
            controller.sequenceGenerator = controller.hazelcast.getCPSubsystem().getAtomicLong(controller.sequenceGeneratorName);
        }

        ///////////////////////
        // Event Store
        ///////////////////////

        public EventSourcingControllerBuilder eventStoreName(String name) {
            controller.eventStoreName = name;
            return this;
        }
//        public EventSourcingControllerBuilder keyName(String name) {
//            controller.keyName = name;
//            return this;
//        }

        // TODO: compaction policy
        // TODO: spill-to-disk enablement (maybe related to compaction policy)

        private void buildEventStore() {
            EventStore es = new EventStore(controller.eventStoreName, controller.hazelcast);
            controller.eventStore = es;
        }

        ///////////////////////
        // Materialized View
        ///////////////////////
        public EventSourcingControllerBuilder viewMapName(String name) {
            controller.viewMapName = name;
            return this;
        }
        private void buildViewMap() {
            controller.viewMap = controller.hazelcast.getMap(controller.viewMapName);
        }

        ///////////////////////
        // Pending Events
        ///////////////////////
        public EventSourcingControllerBuilder pendingEventsMapName(String name) {
            controller.pendingEventsMapName = name;
            return this;
        }
        private void buildPendingMap() {
            // For Viridian Serverless, we need to programmatically enable the map journal
            // as it cannot be toggled on via the UI.  If map config is already set we will not
            // override it.  (This will likely be unnecessary in a future release)
            MapConfig pendingMapConfig = controller.getHazelcast().getConfig().getMapConfig("*_PENDING");
            if (pendingMapConfig == null) {
                EnableMapJournal enabler = new EnableMapJournal("*_PENDING");
                ExecutorService executor = controller.getHazelcast().getExecutorService("Executor");
                executor.submit(enabler);
            }
            controller.pendingEventsMap = controller.hazelcast.getMap(controller.pendingEventsMapName);
        }

        ///////////////////////
        // Pipeline
        ///////////////////////
        private void startPipelineJob() {
            EventSourcingPipeline<D,K> esp = new EventSourcingPipeline(controller);
            controller.pipelineJob = esp.call();
        }


        public EventSourcingController build() {
            logger.info("Building SequenceGenerator");
            buildSequenceGenerator();
            logger.info("Building EventStore");
            buildEventStore();
            logger.info("Building Materialized View/DAO");
            buildViewMap();
            logger.info("Building pending events map");
            buildPendingMap();
            logger.info("Starting pipeline");
            startPipelineJob();;
            return controller;
        }
    }


    public static EventSourcingControllerBuilder newBuilder(HazelcastInstance hz, String domainObjectName) {
        return new EventSourcingControllerBuilder(hz, domainObjectName);
    }

    private EventSourcingController(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
    }

    public String getPendingEventsMapName() { return pendingEventsMapName; }
    public HazelcastInstance getHazelcast() { return hazelcast; }

    public void handleEvent(SourcedEvent<D,K> event) {
        long sequence = getNextSequence();
        PartitionedSequenceKey<K> psk = new PartitionedSequenceKey(sequence, event.getKey());
        //logger.info("handleEvent: PSK(" + psk.getSequence() + "," + psk.getPartitionKey() + ")");
        pendingEventsMap.put(psk, event);
    }

    /** Unclear if this will stick around ... originally added to try to resolve issue in
     * unit tests where shutting down cluster, followed by quick start of new cluster with
     * same config, would throw some weird errors ... so trying to shut down more cleanly
     * in hopes that the newly-started cluster won't complain.
     *
     * So far, this isn't helping at all.
     *
     * example error:
     * [2022-10-19 12:02:31] [WARNING] [192.168.86.35]:5701 [eventsourcing] [5.2-SNAPSHOT]
     * com.hazelcast.spi.exception.RetryableHazelcastException: Cannot submit job with
     * name 'EventSourcing Pipeline for account' before the master node initializes job
     * coordination service's state
     */
    public void shutdown() {
        try {
            pipelineJob.cancel();
            pipelineJob.join();
        } catch (CancellationException ce) {
            // ignore
        }
    }
}
