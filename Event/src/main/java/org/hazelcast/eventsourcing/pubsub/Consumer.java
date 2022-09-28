package org.hazelcast.eventsourcing.pubsub;

public interface Consumer<E> {
    void onEvent(E eventMessage);
}
