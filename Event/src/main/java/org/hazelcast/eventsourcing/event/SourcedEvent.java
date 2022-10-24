package org.hazelcast.eventsourcing.event;

import com.hazelcast.core.HazelcastJsonValue;
import org.hazelcast.eventsourcing.pubsub.SubscriptionManager;

import java.util.function.UnaryOperator;

/** SourcedEvent represents an event that affects a DomainObject.
 *
 * The main features of events in this framework are
 * <BL>
 *     <LI>They are stored in sequential order in the EventStore</LI>
 *     <LI>They are used to build a Materialized View of the Domain Object; an up-to-date
 *     version is kept in the VIEW map at all times, but a materialized view of a domain
 *     object at a particular point in time can be constructed on demand by the various
 *     materialize methods of the EventStore.</LI>
 *     <LI>They are send as messages to other services that interoperate with this one,
 *     using the publish and subscribe methods of the SubscriptionManager.</LI>
 * </BL>
 *
 * Abstract because each Event implementation will need to implement apply() to apply the event
 * to an instance of the domain object.
 *
 * @param <D> the DamainObject class to which this event can be applied
 * @param <K> the key class of the DomainObject (typically but not necessarily a String)
 */
public abstract class SourcedEvent<D extends DomainObject<K>, K> implements UnaryOperator<D> {

    protected K key;
    public K getKey() { return key; }

    /* Event class is needed in order to reconstruct the event when retrieving it via
     * SQL, since SQL doesn't do polymorphism.  This is also the reason that data that
     * is unique to event subclasses is stored in a JSON object rather than having
     * event subclasses define them as member variables.
     */
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
