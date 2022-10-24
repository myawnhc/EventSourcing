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

package org.hazelcast.eventsourcing.eventstore;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlStatement;
import org.hazelcast.eventsourcing.event.DomainObject;
import org.hazelcast.eventsourcing.event.PartitionedSequenceKey;
import org.hazelcast.eventsourcing.event.SourcedEvent;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

/** Hazelcast-centric implementation of an Event Store to support the Event Sourcing
 *  microservice pattern (@see <a href="https://microservices.io/patterns/data/event-sourcing.html">...</a>)
 *  An 'event' is a representation of state changes made to the domain object during the
 *  execution of business logic.  Rather than persisting the current, up-to-date view of
 *  a domain object, in Event Sourcing we instead persist the sequence of state changes
 *  over the history of the object, allowing the current state to be materialized
 *  on demand.  (For real-time use cases, we'll always keep a materialized view built
 *  to support querying, a la the CQRS pattern.
 *
 *  Although the event log is logically an append-only store, it is implemented as
 *  a Hazelcast IMap, with the key being a sequence number and a sorted index
 *  maintained on the item key + sequence compound item.
 *
 * @param <D> the Domain object which is updated by the event sequence
 * @param <K> the type of the key of the domain object
 * @param <E> the Event Object type that will be appended to the Event Store
 */
public class EventStore<D extends DomainObject<K>, K, E extends SourcedEvent<D,K>>
                            implements Serializable, HazelcastInstanceAware {

    transient protected HazelcastInstance hazelcast;
    private static final Logger logger = Logger.getLogger(EventStore.class.getName());

    protected String eventMapName;
    transient protected IMap<PartitionedSequenceKey<K>, E> eventMap;
    transient protected SqlService sqlService;

    private String mapping_template = "CREATE MAPPING IF NOT EXISTS \"?\"\n" +
            "TYPE IMap\n" +
            "OPTIONS (\n" +
            "  'keyFormat' = 'java',\n" +
            "  'keyJavaClass' = 'org.hazelcast.eventsourcing.event.PartitionedSequenceKey',\n" +
            "  'valueFormat' = 'java',\n" +
            "  'valueJavaClass' = 'org.hazelcast.eventsourcing.event.SourcedEvent'\n" +
            ")";

    public EventStore(String mapName, HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
        this.eventMapName = mapName;
        this.eventMap = hazelcast.getMap(mapName);
    }

    /** Append an event to the event store
     *
     * @param key key used to uniquely identify the event
     * @param event the event to be stored
     */
    public void append(PartitionedSequenceKey<K> key, E event) {
        eventMap.set(key, event);
    }


    /** Materialize a view of the domain object using the provided event list.  Internally
     * used by compaction and other materialize methods.
     *
     * @param startingWith a domain object to which events will be applied; this is
     *                     generally produced by calling the domain object constructor
     * @param events a list of events to apply to the starting object to produce the
     *               materialized view
     * @return the materialized domain object
     */
    private D materialize(D startingWith, List<SourcedEvent<D,K>> events) {
        D materializedObject = startingWith;
        for (SourcedEvent<D,K> event: events) {
            materializedObject = event.apply(materializedObject);
        }
        return materializedObject;
    }


    /** Materialize a domain object from the event store.  In normal operation this isn't
     * used as we always keep an up-to-date materialized view, but in a recovery
     * scenario where the in-memory copy is lost this will rebuild it.
     *
     * @param startingWith A domain object to which events will be applied; typically
     *                     built by invoking the domain object's constructor
     * @param keyValue the key value for the domain object's key
     * @param count the maximum number of events to be applied.  Typically only one of count or
     *              upToTimestamp will be supplied.
     * @param upToTimestamp the time which the materialized view is intended to reflect;
     *                      events prior to or equal to this timestamp will be applied, and
     *                      events with later timestamps will be ignored.
     * @return a domain object reflecting the qualifying Events
     */
    private D materialize(D startingWith, K keyValue, int count, long upToTimestamp) {
        List<SourcedEvent<D,K>> events = getEventsFor(keyValue, count, upToTimestamp);
        return materialize(startingWith, events);
    }

    /** Materialize a domain object from the event store.  In normal operation this isn't
     * used as we always keep an up-to-date materialized view, but in a recovery
     * scenario where the in-memory copy is lost this will rebuild it.
     *
     * @param startingWith A domain object to which events will be applied; typically
     *             built by invoking the domain object's constructor
     * @param keyValue the key value for the domain object's key
     * @return a domain object reflecting all events for the domain object
     */
    public D materialize(D startingWith, K keyValue) {
        return materialize(startingWith, keyValue, Integer.MAX_VALUE, Long.MAX_VALUE);
    }

    /** Materialize a domain object from the event store. t.
     *
     * @param startingWith A domain object to which events will be applied; typically
     *                     built by invoking the domain object's constructor
     * @param keyValue the key value for the domain object's key
     * @param count the maximum number of events to be applied.  Typically only one of count or
     *              upToTimestamp will be supplied.
     * @return a domain object reflecting the qualifying Events
     */
    public D materialize(D startingWith, K keyValue, int count) {
        return materialize(startingWith, keyValue, count, Long.MAX_VALUE);
    }

    /** Materialize a domain object from the event store.
     *
     * @param startingWith A domain object to which events will be applied; typically
     *                     built by invoking the domain object's constructor
     * @param keyValue the key value for the domain object's key
     * @param upToTimestamp the time which the materialized view is intended to reflect;
     *                      events prior to or equal to this timestamp will be applied, and
     *                      events with later timestamps will be ignored.
     * @return a domain object reflecting the qualifying Events
     */
    public D materialize (D startingWith, K keyValue, long upToTimestamp) {
        return materialize(startingWith, keyValue, Integer.MAX_VALUE, upToTimestamp);
    }

    private List<SourcedEvent<D,K>> getEventsFor(K keyValue, int count, long upToTimestamp) {
        initSqlService();

        // Substitution of the table name via setParameters not supported as query validator
        // needs to the real table name to verify column names, etc.
        String selectQuery = "select * from " + eventMapName +
                " WHERE CAST(\"key\" AS VARCHAR) = ? ORDER BY __key";
        SqlStatement statement2 = new SqlStatement(selectQuery)
                .setParameters(List.of(keyValue));
        //logger.info("Select Events Query: " + statement2);
        SqlResult result = sqlService.execute(statement2);

        Iterator<SqlRow> iter = result.iterator();
        List<SourcedEvent<D,K>> events = new ArrayList<>();
        while (iter.hasNext()) {
            SqlRow row = iter.next();
            String eventClass = row.getObject("eventClass");
            try {
                Class<? extends SourcedEvent<D,K>> k = (Class<? extends SourcedEvent<D, K>>) Class.forName(eventClass);
                Constructor<? extends SourcedEvent<D, K>> c = k.getConstructor(SqlRow.class);
                SourcedEvent<D,K> event = c.newInstance(row);
                if (event.getTimestamp() >= upToTimestamp)
                    break;
                events.add(event);
                if (events.size() >= count)
                    break;
            } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException |
                     InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
        return events;
    }

    private void initSqlService() {
        if (sqlService == null) {
            sqlService = hazelcast.getSql();
            // CREATE MAPPING doesn't support dynamic parameters, so we do the
            // substitution here.
            mapping_template = mapping_template.replaceAll("\\?", eventMapName);
            sqlService.execute(mapping_template);
        }
    }

    private long getEventCountFor(K keyValue) {
        initSqlService();
        String countQuery = "select COUNT(*) as event_count from " + eventMapName + " WHERE CAST(\"" +
                "key" + "\" AS VARCHAR) = '" + keyValue + "'";
        SqlStatement statement = new SqlStatement(countQuery);
        //logger.info("Count query: " + statement);
        SqlResult result = sqlService.execute(statement);
        Iterator<SqlRow> iter = result.iterator();
        long count = -1;
        while (iter.hasNext()) {
            SqlRow row = iter.next();
            count = row.getObject("event_count");
        }
        return count;
    }

    // We have to iterate through to find the highest sequence number to be deleted
    // since sequence numbers are not per-domain object
    private void deleteOldestEvents(K keyValue, int count) {
        initSqlService();
        String selectOldest = "select __key as psk from " + eventMapName + " WHERE CAST(\"" +
                "key\" AS VARCHAR) = '" + keyValue + "' ORDER BY psk";
        SqlStatement statement = new SqlStatement(selectOldest);
        //logger.info("Select Oldest: " + statement);
        SqlResult result = sqlService.execute(statement);
        Iterator<SqlRow> iter = result.iterator();
        int index = 0;
        while (iter.hasNext()) {
            SqlRow row = iter.next();
            PartitionedSequenceKey<K> psk = row.getObject("psk");
            if (psk.getSequence() == 0) {
                logger.info("Skipping compaction record when deleting old events");
                continue;
            }
            index++;
            eventMap.remove(psk);
            if (index >= count)
                break;
        }
    }

    /** Compact the event store by merging a sequence of events into a single
     * summary record.  Used to reduce the memory footprint of the event store; generally
     * not used when tiered store / spill to disk is enabled for the event store, but since
     * tiered storage is a feature of Hazelcast Enterprise Edition, this is an alternative
     * for open source deployments.
     * @param compactionEvent a newly-initialized compaction event object; since these are
     *                        subclasses of the domain object base class event they cannot
     *                        be instantiated directly by the framework
     * @param domainObject a newly-initialized domain object
     * @param keyValue the key value for which events should be compressed
     * @param compressionFactor the compression factor to use; value must be less than 1 and
     *                          represents the percentage of events to be compressed and
     *                          removed, e.g .50 for 50%, .75 for 75%, etc.
     * @return the number of events which were commpressed and removed (although one new
     * event will be added to the event store which summarizes the content of the removed
     * events)
     */
    public int compact(EventStoreCompactionEvent<D> compactionEvent, D domainObject, K keyValue, double compressionFactor) {
        if (compressionFactor >= 1) {
            throw new IllegalArgumentException("Compression Factor must be < 1");
        }
        long eventCount = getEventCountFor(keyValue);
        int removalCount = (int) (eventCount * compressionFactor);
        logger.info("Current count " + eventCount + " count to remove " + removalCount);
        if (removalCount < 1) return 0;
        List<SourcedEvent<D,K>> eventsToSummarize = getEventsFor(keyValue,
                removalCount, Long.MAX_VALUE);
        domainObject = materialize(domainObject, eventsToSummarize);
        PartitionedSequenceKey<K> psk = new PartitionedSequenceKey<>(0, keyValue);
         compactionEvent.initFromDomainObject(domainObject);
        // Since compaction event isn't processed thru pending -> pipeline, it doesn't get timestamp set
        ((SourcedEvent<D,K>)compactionEvent).setTimestamp(System.currentTimeMillis());
        //compactionEvent.writeAsCheckpoint(domainObject, psk);
        append(psk, (E) compactionEvent);
        deleteOldestEvents(keyValue, removalCount);
        return removalCount;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        // Called on member side as part of pipeline initialization, we have to
        // re-init stuff set on the client side
        this.hazelcast = hazelcastInstance;
        this.eventMap = hazelcast.getMap(eventMapName);
    }

    /** Used by some unit tests; no reason to ever call this in production  */
    public void clearData() {
        this.eventMap.clear();
    }
}
