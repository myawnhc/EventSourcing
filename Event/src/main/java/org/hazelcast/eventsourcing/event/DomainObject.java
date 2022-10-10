package org.hazelcast.eventsourcing.event;

/** Representation of a Domain Object in the Event Sourcing system.  The key relationship between Domain
 * Objects and Events is that a domain object can be created (materialized) by iterating over the set
 * of Events whose key matches the key of the domain object, e.g.
 * <code>
 *     DomainObject dobj = new DomainObject(key);
 *     for (Event e : eventStore.getEventsFor(key)
 *        dobj.apply(e)
 * </code>
 *
 * The above code is for illustration; up-to-date materialized views are maintained by the
 * framework in the [domainObjectName]_VIEW IMap; if the view needs to be recreated it can
 * be done via the EventStore.materialize() method.
 *
 * @param <K> The key type.  Typically a string; for queries to work it must be castable to VARCHAR
 */
public interface DomainObject<K> {
    K getKey();
}
