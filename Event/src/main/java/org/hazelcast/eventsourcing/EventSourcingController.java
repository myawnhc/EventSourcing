package org.hazelcast.eventsourcing;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.map.IMap;
import org.hazelcast.eventsourcing.event.DomainObject;
import org.hazelcast.eventsourcing.event.SourcedEvent;
import org.hazelcast.eventsourcing.eventstore.EventStore;

import java.util.function.Supplier;

/**
 *
 *
 * @param <D> the Domain object which is updated by the event sequence
 * @param <K> the type of the key of the domain object
 * @param <T> the Event Object type that will be appended to the Event Store
 */

public class EventSourcingController <D extends DomainObject<K>, K, T extends SourcedEvent<D,K>> {
    private HazelcastInstance hazelcast;
    private String domainObjectName; // TODO: remove if remains unused ...

    // Sequence Generator
    private String sequenceGeneratorName;
    private IAtomicLong sequenceGenerator;
    public Long getNextSequence() {
        return sequenceGenerator.incrementAndGet();
    }

    // Event Store
    private String eventStoreName;
    private String keyName;
    private Supplier<D> domainObjectConstructor;
    /* Future - settings for compaction and spill-to-disk */
    //private IMap<K, String> eventStoreMap; // TODO: Need parameterized types for K, V
    private EventStore<D, K, T> eventStore;
    public EventStore<D, K, T> getEventStore() {
        return eventStore;
    }

    // TODO: View/DAO Config
    private String viewMapName;
    public String getViewMapName() {
        return viewMapName;
    }
    private IMap<K, DomainObject<K>> viewMap;
    public IMap<K, DomainObject<K>> getViewMap() { return viewMap; }

    // Pending Queue / Map config
    private String pendingEventsMapName;
    private IMap<Long, SourcedEvent> pendingEventsMap; // keyed by sequence

    // TODO: Pipeline Config

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
            // TODO: any default for keyname?  Any consequence if none provided?
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
        public EventSourcingControllerBuilder keyName(String name) {
            controller.keyName = name;
            return this;
        }
        public EventSourcingControllerBuilder domainObjectConstructor(Supplier<? extends DomainObject> cstr) {
            controller.domainObjectConstructor = cstr;
            return this;
        }

        // TODO: compaction policy
        // TODO: spill-to-disk enablement (maybe related to compaction policy)

        private void buildEventStore() {
            // TODO: fail if domainObjectConstructor not set; others have reasonable defaults
            EventStore es = new EventStore(controller.eventStoreName, controller.keyName,
                    controller.domainObjectConstructor, controller.hazelcast);
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

    public void handleEvent(SourcedEvent event) {
        long sequence = getNextSequence();
        pendingEventsMap.put(sequence, event); // TODO: check key, payload here
    }

//    // just for testing ... delete in favor of unit tests
//    public static void main(String[] args) {
//        // Testing only
//        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
//        EventSourcingController esc = EventSourcingController.newBuilder(hz, "account")
//                .sequenceGeneratorName("accountSequence")
//                .build();
//        System.out.println("Next sequence: " + esc.getNextSequence());
//        hz.shutdown();
//    }
}
