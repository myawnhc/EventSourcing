package org.hazelcast.eventsourcing.event;

import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import org.hazelcast.eventsourcing.pubsub.SubscriptionManager;

import java.util.List;
import java.util.function.UnaryOperator;

/**
 * Abstract because each Event implementation will need to implement apply() to apply the event
 * to an instance of the domain object.
 *
 * @param <D> the DamainObject class to which this event can be applied
 */
public abstract class SourcedEvent<D extends DomainObject, K> implements UnaryOperator<D> {

    protected K key;
    public K getKey() { return key; }

    protected long timestamp;
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long value) { timestamp = value; }

    protected GenericRecord payload;
    public GenericRecord getPayload() { return payload; }
    public void setPayload(GenericRecord data) { payload = data; }

    // do we want any status return type here or a future so it doesn't become a blind send?
    public void publish() {
        Class<? extends SourcedEvent> eventClass = this.getClass();
        List<SubscriptionManager> mgrs = SubscriptionManager.getSubscriptionManagers(eventClass);
        for (SubscriptionManager manager : mgrs) {
            manager.publish(eventClass.getCanonicalName(), this);
        }
    }
}
