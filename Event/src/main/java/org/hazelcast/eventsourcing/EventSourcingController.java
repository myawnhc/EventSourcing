package org.hazelcast.eventsourcing;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.map.IMap;
import org.hazelcast.eventsourcing.event.DomainObject;
import org.hazelcast.eventsourcing.event.PartitionedSequenceKey;
import org.hazelcast.eventsourcing.event.SourcedEvent;
import org.hazelcast.eventsourcing.eventstore.EventStore;

/**
 *
 *
 * @param <D> the Domain object which is updated by the event sequence
 * @param <K> the type of the key of the domain object
 * @param <T> the Event Object type that will be appended to the Event Store
 */

public class EventSourcingController <D extends DomainObject<K>, K extends Comparable<K>, T extends SourcedEvent<D,K>> {
    private HazelcastInstance hazelcast;
    private String domainObjectName;
    public String getDomainObjectName() { return domainObjectName; } // used to name pipeline job

    // Sequence Generator
    private String sequenceGeneratorName;
    private IAtomicLong sequenceGenerator;
    public Long getNextSequence() {
        return sequenceGenerator.incrementAndGet();
    }

    // Event Store
    private String eventStoreName;
    /* Future - settings for compaction and spill-to-disk */
    private EventStore<D, K, T> eventStore;
    public EventStore<D, K, T> getEventStore() {
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
    private IMap<PartitionedSequenceKey, SourcedEvent> pendingEventsMap; // keyed by sequence


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
            controller.pendingEventsMap = controller.hazelcast.getMap(controller.pendingEventsMapName);
        }
        ///////////////////////
        // Pipeline
        ///////////////////////
        private void startPipelineJob() {
            EventSourcingPipeline<D,K> esp = new EventSourcingPipeline(controller);
            esp.run();
        }


        public EventSourcingController build() {
            System.out.println("Building SequenceGenerator");
            buildSequenceGenerator();
            System.out.println("Building EventStore");
            buildEventStore();
            System.out.println("Building Materialized View/DAO");
            buildViewMap();
            System.out.println("Building pending events map");
            buildPendingMap();
            System.out.println("Starting pipeline");
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
        pendingEventsMap.put(psk, event);
    }
}
