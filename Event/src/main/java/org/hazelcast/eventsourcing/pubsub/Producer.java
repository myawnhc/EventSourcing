package org.hazelcast.eventsourcing.pubsub;

public interface Producer<E> {
    void publish(E eventMessage);
}
