/*
 * Copyright 2022 Hazelcast, Inc
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package org.hazelcast.eventsourcing.event;

import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import org.hazelcast.eventsourcing.pubsub.SubscriptionManager;

import java.io.Serializable;
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
public abstract class SourcedEvent<D extends DomainObject<K>, K> implements UnaryOperator<GenericRecord>, Serializable {

    protected K key;
    public K getKey() { return key; }

    /* Event name is needed in order to reconstruct the event since it is stored as
     * a GenericRecord in a data store that has mixed types.  The HydrationFactory
     * is responsible for recreating an Event of the appropriate subtype when
     * passed the event name and the GenericRecord
     */

    public static String EVENT_NAME = "eventName"; // used when adding event name to GenericRecord
    public static String EVENT_TIME = "eventTime"; // timestamp of event time in SQL
    public static String EVENT_KEY = "key";

    protected String eventName; // Subclasses must set!
    public String getEventName() { return eventName; }
    public void setEventName(String value) { eventName = value; }

    protected long timestamp;
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long value) { timestamp = value; }

    /** Return a Compact GenericRecord for the event */
    abstract public GenericRecord toGenericRecord();

    public void publish() {
        SubscriptionManager.publish(this);
    }
}
