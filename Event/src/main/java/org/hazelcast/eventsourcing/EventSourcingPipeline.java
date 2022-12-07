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
import com.hazelcast.spi.exception.RetryableHazelcastException;
import org.hazelcast.eventsourcing.event.DomainObject;
import org.hazelcast.eventsourcing.event.PartitionedSequenceKey;
import org.hazelcast.eventsourcing.event.SourcedEvent;
import org.hazelcast.eventsourcing.eventstore.EventStore;

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
        ServiceFactory<?, IMap<K, D>> materializedViewServiceFactory =
                ServiceFactories.iMapService(controller.getViewMapName());

        // Pending map as a service (so we can delete pending events when processed)
        ServiceFactory<?, IMap<PartitionedSequenceKey<K>, SourcedEvent<D,K>>> pendingMapServiceFactory =
                ServiceFactories.iMapService(controller.getPendingEventsMapName());

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.mapJournal(pendingEventsMapName, JournalInitialPosition.START_FROM_OLDEST))
                .withoutTimestamps()
                // Update SourcedEvent with timestamp.  Forcing local parallelism to 1 for this
                // stage as otherwise we may receive events out of order; in most cases this is
                // OK but in a few cases, like account transasction ahead of account being opened,
                // can lead to failures
                .map(pendingEvent -> {
                    PartitionedSequenceKey<K> psk = (PartitionedSequenceKey<K>) pendingEvent.getKey();
                    //logger.info("Pipeline received pendingEvent with PSK(" + psk.getSequence() + "," + psk.getPartitionKey() + ")");
                    SourcedEvent<D,K> event = (SourcedEvent<D,K>) pendingEvent.getValue();
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
                    SourcedEvent<D,K> event = tuple2.f1();
                    pendingMap.delete(event.getKey());
                    return event;
                }).setName("Remove event from pending events")
                .writeTo(Sinks.noop());

        return p;
    }
}
