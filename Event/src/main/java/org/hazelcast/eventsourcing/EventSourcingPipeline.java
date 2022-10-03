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
import org.hazelcast.eventsourcing.event.DomainObject;
import org.hazelcast.eventsourcing.event.PartitionedSequenceKey;
import org.hazelcast.eventsourcing.event.SourcedEvent;
import org.hazelcast.eventsourcing.eventstore.EventStore;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

/**
 *
 * @param <D> Domain object type
 * @param <K> Domain object key type
 */
public class EventSourcingPipeline<D extends DomainObject<K>, K> implements Runnable {

    private EventSourcingController controller;
    private String pendingEventsMapName;

    public EventSourcingPipeline(EventSourcingController controller) {
        this.controller = controller;
        this.pendingEventsMapName = controller.getPendingEventsMapName();
    }

    @Override
    public void run() {
        Pipeline p = createPipeline();
        JobConfig jobConfig = new JobConfig();
        // TODO: add jars as needed
        jobConfig.setName("EventSourcing Pipeline for " + controller.getDomainObjectName());
        Job j = controller.getHazelcast().getJet().newJobIfAbsent(p, jobConfig);
    }

    public Pipeline createPipeline() {

        // Event Store as a service
        EventStore eventStore = controller.getEventStore();
        ServiceFactory<?, EventStore> eventStoreServiceFactory =
                ServiceFactories.sharedService((ctx) -> eventStore);

        // IMap/Materialized View as a service --
        ServiceFactory<?, IMap<K, D>> materializedViewServiceFactory =
                ServiceFactories.iMapService(controller.getViewMapName());

        Pipeline p = Pipeline.create();
        p.setPreserveOrder(true); // trying to impose order on event journal
        p.readFrom(Sources.mapJournal(pendingEventsMapName, JournalInitialPosition.START_FROM_OLDEST))
                .withIngestionTimestamps()
                // Update SourcedEvent with timestamp
                .map(pendingEvent -> {
                    PartitionedSequenceKey<K> psk = (PartitionedSequenceKey) pendingEvent.getKey();
                    //Long sequence = psk.getSequence();
                    //System.out.println("Received pendingEvent with sequence " + sequence);
                    SourcedEvent<D,K> event = (SourcedEvent<D,K>) pendingEvent.getValue();
                    event.setTimestamp(System.currentTimeMillis());
                    return tuple2(psk, event);
                }).setName("Update SourcedEvent with timestamp")
                // Append to event store
                .mapUsingService(eventStoreServiceFactory, (eventstore, tuple2) -> {
                    PartitionedSequenceKey<K> key = tuple2.f0();
                    SourcedEvent<D,K> event = tuple2.f1();
                    eventstore.append(key, event);
                    return event;
                }).setName("Persist Event to event store")
                // Update materialized view (using EntryProcessor)
                .mapUsingService(materializedViewServiceFactory, (viewMap, event) -> {
                    viewMap.executeOnKey(event.getKey(), (EntryProcessor<K, D, D>) viewEntry -> {
                        D domainObject = viewEntry.getValue();
                        //K domainObjectKey = viewEntry.getKey();
                        domainObject = event.apply(domainObject);
                        viewEntry.setValue(domainObject);
                        return domainObject;
                    });
                    return event;
                }).setName("Update Materialized View")
                // Broadcast the event to all subscribers
                .map(event -> {
                    event.publish();
                    return event;
                }).setName("Publish event to all subscribers")
                .writeTo(Sinks.logger());

        return p;
    }
}
