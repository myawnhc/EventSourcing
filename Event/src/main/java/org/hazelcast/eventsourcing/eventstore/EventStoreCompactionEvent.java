package org.hazelcast.eventsourcing.eventstore;

// Interface to be implemented by SourcedEvent subclass that handles compaction for an
// EventStore
public interface EventStoreCompactionEvent<D> {
    void initFromDomainObject(D domainObject);
}
