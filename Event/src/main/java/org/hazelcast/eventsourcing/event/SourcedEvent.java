package org.hazelcast.eventsourcing.event;

import com.hazelcast.core.HazelcastJsonValue;
import org.hazelcast.eventsourcing.pubsub.SubscriptionManager;

import java.util.function.UnaryOperator;

/**
 * Abstract because each Event implementation will need to implement apply() to apply the event
 * to an instance of the domain object.
 *
 * @param <D> the DamainObject class to which this event can be applied
 */
public abstract class SourcedEvent<D extends DomainObject<K>, K> implements UnaryOperator<D> {

    protected K key;
    public K getKey() { return key; }

    protected String eventClass;
    public String getEventClass() { return eventClass; }
    public void setEventClass(String value) { eventClass = value; }

    protected long timestamp;
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long value) { timestamp = value; }

    protected HazelcastJsonValue payload;
    public HazelcastJsonValue getPayload() { return payload; }
    public void setPayload(HazelcastJsonValue data) { payload = data; }

    public void publish() {
        SubscriptionManager.publish(this);
    }
}
