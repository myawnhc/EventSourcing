package org.hazelcast.eventsourcing.pubsub;

/** Event sourcing needs a pub-sub mechanism to allow different microservices to be
 * notified of events of interest on services with which they interoperate.  This
 * interface represents the Producer / Writer side of the pub-sub connection.
 *
 * @param <E> base class of events to be passed as messages
 */
public interface Producer<E> {
    /** Publish the message to all subscribers */
    void publish(E eventMessage);
}
