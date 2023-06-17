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
    GenericRecord toGenericRecord();
}
