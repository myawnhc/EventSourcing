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
import org.hazelcast.eventsourcing.event.HydrationFactory;
import org.hazelcast.eventsourcing.event.PartitionedSequenceKey;
import org.hazelcast.eventsourcing.event.SourcedEvent;
import org.hazelcast.eventsourcing.eventstore.EventStore;
import org.hazelcast.eventsourcing.sync.CompletionInfo;

import java.util.concurrent.Callable;
import java.util.logging.Logger;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

/** Pipeline code to implement the handleEvent function of the controller.
 *  Triggered by the addition of events to the Pending Events Map.
 *
 * @param <D> Domain object type
 * @param <K> Domain object key type
 * @param <E> Base class for events that will be handled in this pipeline
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
        Pipeline p = createPipeline();
        JobConfig jobConfig = new JobConfig();
        // future: add jars as needed for client-server deployment
        jobConfig.setName("EventSourcing Pipeline for " + controller.getDomainObjectName());
        while (true) {
            try {
                return controller.getHazelcast().getJet().newJobIfAbsent(p, jobConfig);
            } catch (RetryableHazelcastException rte) {
                // Seen: com.hazelcast.spi.exception.RetryableHazelcastException:
                // Cannot submit job with name '<jobname>' before the master node initializes job coordination service's state
                logger.warning("Retrying job submission due to retryable exception");
            }
        }
    }

    private Pipeline createPipeline() {

        // Event Store as a service
        EventStore<D,K,E> eventStore = controller.getEventStore();
        ServiceFactory<?, EventStore<D,K,E>> eventStoreServiceFactory =
                ServiceFactories.sharedService((ctx) -> eventStore);

        // IMap/Materialized View as a service --
        ServiceFactory<?, IMap<K, GenericRecord>> materializedViewServiceFactory =
                ServiceFactories.iMapService(controller.getViewMapName());

        // Pending map as a service (so we can delete pending events when processed)
        ServiceFactory<?, IMap<PartitionedSequenceKey<K>,GenericRecord>> pendingMapServiceFactory =
                ServiceFactories.iMapService(controller.getPendingEventsMapName());

        // Completions map, used in Sink
        String completionsMapName = controller.getCompletionMapName();
        IMap<PartitionedSequenceKey, CompletionInfo> completionsMap =
                controller.getHazelcast().getMap(completionsMapName);

        HydrationFactory<D,K,E> hydrationFactory = controller.getEventStore().getHydrationFactory();

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.mapJournal(pendingEventsMapName, JournalInitialPosition.START_FROM_OLDEST))
                .withoutTimestamps()
                // Update SourcedEvent with timestamp.  Forcing local parallelism to 1 for this
                // stage as otherwise we may receive events out of order; in most cases this is
                // OK but in a few cases, like account transaction ahead of account being opened,
                // can lead to failures
                .map(pendingEvent -> {
                    PartitionedSequenceKey<K> psk = (PartitionedSequenceKey<K>) pendingEvent.getKey();
                    //logger.info("Pipeline received pendingEvent with PSK(" + psk.getSequence() + "," + psk.getPartitionKey() + ")");
                    //SourcedEvent<D,K> event = (SourcedEvent<D,K>) pendingEvent.getValue();
                    GenericRecord eventgr = (GenericRecord) pendingEvent.getValue();
                    String eventName = eventgr.getString("eventName");
                    SourcedEvent<D,K> event = hydrationFactory.hydrateEvent(eventName, eventgr);
                    event.setTimestamp(System.currentTimeMillis());
                    return tuple2(psk, event);
                }).setLocalParallelism(1).setName("Update SourcedEvent with timestamp")
                // Append to event store
                .mapUsingService(eventStoreServiceFactory, (eventstore, tuple2) -> {
                    PartitionedSequenceKey<K> key = tuple2.f0();
                    SourcedEvent<D,K> event = tuple2.f1();
                    eventstore.append(key, event);
                    return tuple2;
                }).setName("Persist Event to event store")
                // Update materialized view (using EntryProcessor)
                .mapUsingService(materializedViewServiceFactory, (viewMap, tuple2) -> {
                    // TODO: this stage occasionally reports a NPE (no line # info available)
                    SourcedEvent<D,K> event = tuple2.f1();
                    //logger.info("EventSourcingPipeline updating materialized view for event " + event);
                    viewMap.executeOnKey(event.getKey(), (EntryProcessor<K,GenericRecord,GenericRecord>) viewEntry -> {
                        GenericRecord domainObjectGR = viewEntry.getValue();
                        //K domainObjectKey = viewEntry.getKey(); // NOT NEEDED
                        D domainObject = null; // OK for events that do the initial DO creation
                        if (domainObjectGR != null) {
                            domainObject = hydrationFactory.hydrateDomainObject(domainObjectGR);
                            //logger.info("  hydrated domain object " + domainObjectGR);
                        }
//                        else {
//                            logger.info("  null domain object (OK for CREATE type events");
//                        }
                        domainObject = event.apply(domainObject);
                        //logger.info("  domain object after applying event: " + domainObject);
                        viewEntry.setValue(domainObject.toGenericRecord());
                        return domainObject.toGenericRecord();
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
                        //SourcedEvent<D,K> removed = pendingMap.remove(key);
                        GenericRecord removed = pendingMap.remove(key);
                        if (removed == null) {
                            logger.warning("Failed to remove pending event with key " + key);
                        }
                    } else {
                        pendingMap.delete(key);
                    }
                    return tuple2;
                }).setName("Remove event from pending events")
                .writeTo(Sinks.mapWithUpdating(completionsMap,
                        tuple -> tuple.f0(),
                        (completionInfo, tuple) -> {
                            //System.out.println("Updating " + completionsMapName + " in sink");
                            if (completionInfo == null) {
                                logger.warning("No completion info to update for key " + tuple.f0());
                                return null;
                            } else {
                                completionInfo.markComplete();
                                return completionInfo;
                            }
                        })).setName("Update completion info");

        return p;
    }
}
