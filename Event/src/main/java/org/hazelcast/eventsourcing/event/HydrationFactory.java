/*
 * Copyright 2023 Hazelcast, Inc
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
import com.hazelcast.sql.SqlRow;

/** HydrationFactory implementation should be provided by the service using the
 * Event Sourcing functionality and passed to the event store.  Used to recreate
 * both domain objects and event objects from their serialized format (which is
 * a Compact GenericRecord
 *
 * @param <D> Domain Object class (should be just one per service)
 * @param <K> Key type of the Domain Object Class
 * @param <E> Base class for all events targeted to the EventStore
 */
public interface HydrationFactory<D extends DomainObject<K>, K, E extends SourcedEvent<D,K>> {
    D hydrateDomainObject(GenericRecord data);

    E hydrateEvent(String eventName, SqlRow data);
    E hydrateEvent(String eventName, GenericRecord data);

    /** SQL CREATE MAPPING statement used to create the IMap schema for the event
     * store.  The columns declared in the mapping should be the union of all
     * fields from the object subtypes so that any object type can be stored
     * in the common EventStore
     *
     * @param eventStoreName
     * @return
     */
    String getEventMapping(String eventStoreName);
}
