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

package org.hazelcast.eventsourcing;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.jet.Job;
import com.hazelcast.map.IMap;
import org.hazelcast.eventsourcing.event.DomainObject;
import org.hazelcast.eventsourcing.event.PartitionedSequenceKey;
import org.hazelcast.eventsourcing.event.SourcedEvent;
import org.hazelcast.eventsourcing.eventstore.EventStore;
import org.hazelcast.eventsourcing.sync.CompletionInfo;
import org.hazelcast.eventsourcing.sync.CompletionInfoCompactSerializer;
import org.hazelcast.eventsourcing.sync.CompletionTracker;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
    private final HazelcastInstance hazelcast;
    private String domainObjectName;
    /** Return the name of the domain object passed in to the Builder */
    public String getDomainObjectName() { return domainObjectName; } // used to name pipeline job
    private static Logger logger = null; // assigned in static initializer
    // Used by service pipelines to receive completion notifications from ESPipeline
    private CompletionTracker completionTracker = new CompletionTracker();
    public CompletionTracker getCompletionTracker() { return completionTracker; }

    // Jars that need to be attached to JobConfig of the EventSourcingPipelines
    private List<URL> dependendencies;
    public List<URL> getDependentJars() { return dependendencies; }

    // Sequence Generator
    private String sequenceGeneratorName;
    private IAtomicLong sequenceGenerator;
    /** Return the next sequence number to be used for events */
    private Long getNextSequence() {
        return sequenceGenerator.incrementAndGet();
    }

    // Event Store
    private String eventStoreName;
    /* Future - settings for compaction and spill-to-disk */
    private EventStore<D, K, E> eventStore;

    /** Return a reference to the EventStore.  Useful if you need to materialize an
     * object with a subset of the published events, but in typical operation there
     * isn't a need to access the event store directly.
     * @return the EventStore associated with this controller.
     */
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
    private IMap<PartitionedSequenceKey<K>, SourcedEvent<D,K>> pendingEventsMap;
    public String getPendingEventsMapName() { return pendingEventsMapName; }

    // Completions map
    private String completionMapName;
    private IMap<PartitionedSequenceKey<K>, CompletionInfo> completionsMap;
    public String getCompletionMapName() { return completionMapName; }

    // Pipeline
    private Job pipelineJob;

    private String pending_mapping_template = "CREATE MAPPING IF NOT EXISTS \"?\"\n" +
            "TYPE IMap\n" +
            "OPTIONS (\n" +
            "  'keyFormat' = 'java',\n" +
            "  'keyJavaClass' = 'org.hazelcast.eventsourcing.event.PartitionedSequenceKey',\n" +
            "  'valueFormat' = 'java',\n" +
            "  'valueJavaClass' = 'org.hazelcast.eventsourcing.event.SourcedEvent'\n" +
            ")";

    private String completions_mapping_template = "CREATE MAPPING IF NOT EXISTS \"?\"\n" +
            "TYPE IMap\n" +
            "OPTIONS (\n" +
            "  'keyFormat' = 'java',\n" +
            "  'keyJavaClass' = 'org.hazelcast.eventsourcing.event.PartitionedSequenceKey',\n" +
            "  'valueFormat' = 'java',\n" +
            "  'valueJavaClass' = 'org.hazelcast.eventsourcing.sync.CompletionInfo'\n" +
            ")";


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

    // Helper method to add any additions we require to the Config object before the service
    // starts up the cluster
    public static Config addRequiredConfigItems(Config in) {
        SerializationConfig sc = in.getSerializationConfig();
        CompactSerializationConfig csc = sc.getCompactSerializationConfig().addSerializer(new CompletionInfoCompactSerializer());
        sc.setCompactSerializationConfig(csc);
        in.setSerializationConfig(sc);
        return in;
    }

    public static ClientConfig addRequiredConfigItems(ClientConfig in) {
        SerializationConfig sc = in.getSerializationConfig();
        CompactSerializationConfig csc = sc.getCompactSerializationConfig().addSerializer(new CompletionInfoCompactSerializer());
        sc.setCompactSerializationConfig(csc);
        in.setSerializationConfig(sc);
        return in;
    }

    /////////////
    // Builder
    /////////////
    public static class EventSourcingControllerBuilder<D extends DomainObject<K>, K extends Comparable<K>, E extends SourcedEvent<D,K>> {
        private final EventSourcingController<D,K,E> controller;

        public EventSourcingControllerBuilder(HazelcastInstance hazelcast, String domainObjectName) {
            this.controller = new EventSourcingController<>(hazelcast);
            this.controller.domainObjectName = domainObjectName;
            // Set default names
            this.controller.sequenceGeneratorName = domainObjectName + "_SEQ";
            this.controller.eventStoreName = domainObjectName + "_ES";
            this.controller.pendingEventsMapName = domainObjectName + "_PENDING";
            this.controller.viewMapName = domainObjectName + "_VIEW";
            this.controller.completionMapName = domainObjectName + "_COMPLETIONS";
        }

        ///////////////////////
        // Sequence Generator
        ///////////////////////
        public EventSourcingControllerBuilder<D,K,E> sequenceGeneratorName(String name) {
            controller.sequenceGeneratorName = name;
            return this;
        }

        private void buildSequenceGenerator() {
            controller.sequenceGenerator = controller.hazelcast.getCPSubsystem().getAtomicLong(controller.sequenceGeneratorName);
        }

        ///////////////////////
        // Event Store
        ///////////////////////

        public EventSourcingControllerBuilder<D,K,E> eventStoreName(String name) {
            controller.eventStoreName = name;
            return this;
        }

        // TODO: compaction policy
        // TODO: spill-to-disk enablement (maybe related to compaction policy)

        private void buildEventStore() {
            controller.eventStore = new EventStore<>(controller.eventStoreName, controller.hazelcast);
        }

        ///////////////////////
        // Materialized View
        ///////////////////////
        public EventSourcingControllerBuilder<D,K,E> viewMapName(String name) {
            controller.viewMapName = name;
            return this;
        }
        private void buildViewMap() {
            controller.viewMap = controller.hazelcast.getMap(controller.viewMapName);
        }

        ///////////////////////
        // Pending Events
        ///////////////////////
        public EventSourcingControllerBuilder<D,K,E> pendingEventsMapName(String name) {
            controller.pendingEventsMapName = name;
            return this;
        }
        private void buildPendingMap() {
            // For Viridian Serverless, we need to programmatically enable the map journal
            // as it cannot be toggled on via the UI.  If map config is already set we will not
            // override it.  (This will likely be unnecessary in a future release)
//            MapConfig pendingMapConfig = controller.getHazelcast().getConfig().getMapConfig("*_PENDING");
//            if (pendingMapConfig == null) {
//                EnableMapJournal enabler = new EnableMapJournal("*_PENDING");
//                ExecutorService executor = controller.getHazelcast().getExecutorService("Executor");
//                executor.submit(enabler);
//            }
            controller.pendingEventsMap = controller.hazelcast.getMap(controller.pendingEventsMapName);
            // We don't use this SQL mapping internally but define it to make it friendlier to developers
            controller.pending_mapping_template = controller.pending_mapping_template.replaceAll("\\?", controller.pendingEventsMapName);
            controller.getHazelcast().getSql().execute(controller.pending_mapping_template);
        }

        ///////////////////////
        // Completions
        ///////////////////////
        public EventSourcingControllerBuilder<D,K,E> completionsMapName(String name) {
            controller.completionMapName = name;
            return this;
        }
        private void buildCompletionsMap() {
            controller.completionsMap = controller.hazelcast.getMap(controller.completionMapName);
            controller.completionsMap.addEntryListener(controller.getCompletionTracker(), true);
            // We don't use this SQL mapping internally but define it to make it friendlier to developers
            controller.completions_mapping_template = controller.completions_mapping_template.replaceAll("\\?", controller.completionMapName);
            controller.getHazelcast().getSql().execute(controller.completions_mapping_template);
        }

        ///////////////////////
        // Pipeline
        ///////////////////////
        private void startPipelineJob() {
            EventSourcingPipeline<D,K,E> esp = new EventSourcingPipeline<>(controller);
            controller.pipelineJob = esp.call();
        }

        // Dependencies of pipeline
        public EventSourcingControllerBuilder<D,K,E> addDependencies(List<URL> dependentJars) {
            controller.dependendencies = dependentJars;
            return this;
        }



        public EventSourcingController<D,K,E> build() {
            logger.info("Building SequenceGenerator");
            buildSequenceGenerator();
            logger.info("Building EventStore");
            buildEventStore();
            logger.info("Building maps");
            buildViewMap();
            buildPendingMap();
            buildCompletionsMap();
            logger.info("Starting pipeline");
            startPipelineJob();
            return controller;
        }
    }

    /** Return a builder that can be used to instantiate a new controller.  The builder allows control
     * over the names of the various structures built by the controller, but these all have reasonable
     * defaults so in most cases, instantiating the builder with the domain object name and then
     * calling build() will be all that is needed.
     *
     * @param hz Hazelast instance that will host the data structures
     * @param domainObjectName name of the domain object; all lower-case is preferred.
     * @return a Builder initialized with reasonable defaults for all controller object names.
     * @param <D>
     * @param <K>
     * @param <E>
     */
    public static <D extends DomainObject<K>, K extends Comparable<K>,
                   E extends SourcedEvent<D,K>> EventSourcingControllerBuilder<D,K,E>
        newBuilder(HazelcastInstance hz, String domainObjectName) {
            return new EventSourcingControllerBuilder<D,K,E>(hz, domainObjectName);
    }

    private EventSourcingController(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
    }


    public HazelcastInstance getHazelcast() { return hazelcast; }

//    /** Handle the event.  The event will be inserted into the Pending Events map for the
//     * domain object, which will in turn trigger the pipeline job that handles the
//     * event.  See the EventSourcingPipeline for details, but basic steps in handling the
//     * event include
//     * <bl>
//     *     <li>Assign a unique sequence number to the event and timestamp it</li>
//     *     <li>Append the event to the event store</li>
//     *     <li>Update the domain object's materialized view with the event </li>
//     *     <li>Notify any observers/subscribers to the event that the event occurred</li>
//     *     <li>Remove the event from the pending events map</li>
//     * </bl>
//     * @param event
//     */
//    @Deprecated
//    public PartitionedSequenceKey<K> handleEvent(SourcedEvent<D,K> event) {
//        long sequence = getNextSequence();
//        PartitionedSequenceKey<K> psk = new PartitionedSequenceKey<>(sequence, event.getKey());
//        logger.info("DEPRECATED handleEvent: PSK(" + psk.getSequence() + "," + psk.getPartitionKey() + ")");
//        pendingEventsMap.put(psk, event);
//        completionsMap.put(psk, new CompletionInfo());
//        return psk;
//    }
    @Deprecated
    /** @Deprecated use the variant that takes a UUID as second argument */
    public CompletableFuture<CompletionInfo> handleEvent(SourcedEvent<D,K> event) {
        System.err.println("WARNING: single-arg version of handleEvent is deprecated - shuld pass UUID as arg1");
        return handleEvent(event, null);
    }


    public CompletableFuture<CompletionInfo> handleEvent(SourcedEvent<D,K> event, UUID identifier) {
        try {
            long sequence = getNextSequence();
            PartitionedSequenceKey<K> psk = new PartitionedSequenceKey<>(sequence, event.getKey());
            CompletableFuture<CompletionInfo> future = completionTracker.register(psk);
            completionsMap.put(psk, new CompletionInfo(event, identifier));
            pendingEventsMap.put(psk, event); // Triggers the EventSourcingPipeline
            return future;
        } catch (Throwable t) {
            t.printStackTrace();
            return null;
        }
    }
}
