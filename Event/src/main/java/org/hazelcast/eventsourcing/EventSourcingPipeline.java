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

import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import org.hazelcast.eventsourcing.event.DomainObject;
import org.hazelcast.eventsourcing.event.PartitionedSequenceKey;
import org.hazelcast.eventsourcing.event.SourcedEvent;
import org.hazelcast.eventsourcing.eventstore.EventStore;
import org.hazelcast.eventsourcing.sync.CompletionInfo;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

/** Pipeline code to implement the handleEvent function of the controller.
 *  Triggered by the addition of events to the Pending Events Map.
 *
 * @param <D> Domain object type
 * @param <K> Domain object key type
 */
public class EventSourcingPipeline<D extends DomainObject<K>, K extends Comparable<K>, E extends SourcedEvent<D,K>> implements Callable<Job> {

    private final EventSourcingController<D,K,E> controller;
    private final String pendingEventsMapName;
    private static final Logger logger = Logger.getLogger(EventSourcingPipeline.class.getName());

    public EventSourcingPipeline(EventSourcingController<D,K,E> controller) {
        this.controller = controller;
        this.pendingEventsMapName = controller.getPendingEventsMapName();
    }

    /** Create the pipeline job and submit it for execution.
     *
     * @return the Job submitted to the cluster
     */
    @Override
    public Job call() {
        try {
            Pipeline p = createPipeline();
            JobConfig jobConfig = new JobConfig();
            // Because we're run from a service, our jar location cannot be determined relative
            // to where the app was launched.  The classloader knows where we were loaded from
            // so we can get our enclosing jar from the associated resource RUL.
//            URL curl = EventSourcingPipeline.class.getResource("EventSourcingPipeline.class");
//            String filePart = curl.getFile();
//            System.out.println("-----> Parsing " + curl);
//            int sepIndex = filePart.indexOf('!');
//            URL eventSourcing = null;
//            if (sepIndex == -1) {
//                System.out.println("Class outside of jar, using URL as-is");
//                eventSourcing = curl;
//            } else {
//                String jarPart = filePart.substring(0, sepIndex);
//                System.out.println("File inside jar, using URL " + jarPart);
//                eventSourcing = URI.create(jarPart).toURL();
//            }
            jobConfig.setName("EventSourcing Pipeline for " + controller.getDomainObjectName());
            //System.out.println("- EventSourcingPipeline JobConfig includes " + eventSourcing);
            for (URL url : controller.getDependentJars())
                jobConfig.addJar(url);

//            List<CompactSerializer> serializers = new ArrayList<>();
//            for (CompactSerializer<? extends SourcedEvent> cs : serializers) {
//                // Expects StreamSerializer, we have CompactSerializer ...
//                jobConfig.registerSerializer(cs.getCompactClass(), cs.getClass());
//            }

            while (true) {
                try {
                    return controller.getHazelcast().getJet().newJobIfAbsent(p, jobConfig);
                } catch (RetryableHazelcastException rte) {
                    // Seen: com.hazelcast.spi.exception.RetryableHazelcastException:
                    // Cannot submit job with name '<jobname>' before the master node initializes job coordination service's state
                    logger.warning("Retrying job submission due to retryable exception");
                }
            }
        } catch (Throwable m) {
            m.printStackTrace();
            return null;
        }
    }

    private Pipeline createPipeline() {

        // Event Store as a service
        EventStore<D,K,E> eventStore = controller.getEventStore();
        ServiceFactory<?, EventStore<D,K,E>> eventStoreServiceFactory =
                ServiceFactories.sharedService((ctx) -> eventStore);

        // IMap/Materialized View as a service --
        ServiceFactory<?, IMap<K, D>> materializedViewServiceFactory =
                ServiceFactories.iMapService(controller.getViewMapName());

        // Pending map as a service (so we can delete pending events when processed)
        ServiceFactory<?, IMap<PartitionedSequenceKey<K>, SourcedEvent<D,K>>> pendingMapServiceFactory =
                ServiceFactories.iMapService(controller.getPendingEventsMapName());

        // Completions map, used in Sink
        String completionsMapName = controller.getCompletionMapName();
        IMap<PartitionedSequenceKey, CompletionInfo> completionsMap =
                controller.getHazelcast().getMap(completionsMapName);

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.mapJournal(pendingEventsMapName, JournalInitialPosition.START_FROM_OLDEST))
                .withoutTimestamps()
                // Update SourcedEvent with timestamp.  Forcing local parallelism to 1 for this
                // stage as otherwise we may receive events out of order; in most cases this is
                // OK but in a few cases, like account transasction ahead of account being opened,
                // can lead to failures
                .map(pendingEvent -> {
                    PartitionedSequenceKey<K> psk = (PartitionedSequenceKey<K>) pendingEvent.getKey();
                    logger.info("EventSourcingPipeline received pendingEvent with PSK(" + psk.getSequence() + "," + psk.getPartitionKey() + ")");
                    SourcedEvent<D,K> event;
                    // Can't do instanceof test here -- getValue triggers deserialization which
                    // will fail if SourcedEvent subclass CompactSerializer isn't registered
                    if (pendingEvent.getValue() instanceof SourcedEvent)
                        event = (SourcedEvent<D,K>) pendingEvent.getValue();
                    else {
                        // Experimental - only implemented for OpenAccountEvent at this time
                        System.out.println("Reconstructing event from generic record " + pendingEvent.getValue());
                        GenericRecord r = (GenericRecord) pendingEvent.getValue();
                        String eventClass = r.getString("eventClass");
                        // Requests OpenAccountEvent on classpath - if we had that, we wouldn't be in this
                        // fork to begin with!
                        Class<? extends SourcedEvent<D,K>> k = (Class<? extends SourcedEvent<D, K>>) Class.forName(eventClass);
                        // key, timestamp, payload, eventClass are member fields
                        Constructor<? extends SourcedEvent<D, K>> c = k.getConstructor(GenericRecord.class);
                        event = c.newInstance(r);
                    }
                    event.setTimestamp(System.currentTimeMillis());
                    return tuple2(psk, event);
                }).setLocalParallelism(1).setName("Update SourcedEvent with timestamp")
                // Append to event store
                .mapUsingService(eventStoreServiceFactory, (eventstore, tuple2) -> {
                    PartitionedSequenceKey<K> key = tuple2.f0();
                    SourcedEvent<D,K> event = tuple2.f1();
                    eventstore.append(key, (E) event);
                    return tuple2;
                }).setName("Persist Event to event store")
                // Update materialized view (using EntryProcessor)
                .mapUsingService(materializedViewServiceFactory, (viewMap, tuple2) -> {
                    SourcedEvent<D,K> event = tuple2.f1();
                    viewMap.executeOnKey(event.getKey(), (EntryProcessor<K, D, D>) viewEntry -> {
                        D domainObject = viewEntry.getValue();
                        //K domainObjectKey = viewEntry.getKey();
                        domainObject = event.apply(domainObject);
                        viewEntry.setValue(domainObject);
                        return domainObject;
                    });
                    return tuple2;
                }).setName("Update Materialized View")
                // Broadcast the event to all subscribers
                .map(tuple2 -> {
                    SourcedEvent<D,K> event = tuple2.f1();
                    event.publish();
                    return tuple2;
                }).setName("Publish event to all subscribers")
                // Delete event from the pending events map
                .mapUsingService(pendingMapServiceFactory, (pendingMap, tuple2) -> {
                    boolean debugRemoval = true;
                    PartitionedSequenceKey<K> key = tuple2.f0();
                    SourcedEvent<D,K> event = tuple2.f1(); // used only as return which goes to nop stage
                    if (debugRemoval) {
                        SourcedEvent<D,K> removed = pendingMap.remove(key);
                        if (removed == null) {
                            logger.warning("Failed to remove pending event with key " + key);
                        }
                    } else {
                        pendingMap.delete(key);
                    }
                    return tuple2;
                }).setName("Remove event from pending events")
                .writeTo(Sinks.mapWithUpdating(completionsMap,
                        Tuple2::f0,
                        (completionInfo, tuple) -> {
                            if (completionInfo == null) {
                                logger.warning("No completion info to update for key " + tuple.f0());
                                return null;
                            } else {
                                completionInfo.markComplete();
                                System.out.println("CI for " + tuple.f0() + " updated in ESP sink: " + completionInfo + " : " + completionInfo.status);
                                return completionInfo;
                            }
                        })).setName("Update completion info");

        return p;
    }
}
