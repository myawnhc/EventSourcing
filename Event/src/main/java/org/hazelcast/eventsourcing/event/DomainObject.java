package org.hazelcast.eventsourcing.event;

/** Representation of a Domain Object in the Event Sourcing system.  The key relationship between Domain
 * Objects and Events is that a domain object can be created (materialized) by iterating over the set
 * of Events whose key matches the key of the domain object, e.g.
 * <code>
 *     DomainObject dobj = new DomainObject(key);
 *     for (Event e : eventStore.getEventsFor(key)
 *        dobj.apply(e)
 * </code>
 * @param <K>
 */
public interface DomainObject<K> {
    K getKey();
}
