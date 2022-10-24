package org.hazelcast.eventsourcing.pubsub;

/** Event sourcing needs a pub-sub mechanism to allow different microservices to be
 * notified of events of interest on services with which they interoperate.  This
 * interface represents the Consumer / Reader side of the pub-sub connection.
 *
 * @param <E> base class of events to be passed as messages
 */
public interface Consumer<E> {
    /** Callback implemented by consumers to receive message notifications */
    void onEvent(E eventMessage);
}
