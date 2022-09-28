package org.hazelcast.eventsourcing.eventstore;

import org.hazelcast.eventsourcing.event.SourcedEvent;

import java.io.Serializable;

// experimental
@Deprecated
public record EventEntry(long sequence, long timestamp, SourcedEvent event) implements Serializable {
    // TODO: make this CompactSerializable for improved performance (will need to do SourcedEvent as well)
}
