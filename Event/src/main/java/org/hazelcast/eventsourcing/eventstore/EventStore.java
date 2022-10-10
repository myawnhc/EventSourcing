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

/** Hazelcast-centric implementation of an Event Store to support the Event Sourcing
 *  microservice pattern (@see https://microservices.io/patterns/data/event-sourcing.html)
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
 * @param <T> the Event Object type that will be appended to the Event Store
 */
public class EventStore<D extends DomainObject<K>, K, T extends SourcedEvent<D,K>>
                            implements Serializable, HazelcastInstanceAware {

    transient protected HazelcastInstance hazelcast;

    protected String eventMapName;
    transient protected IMap<PartitionedSequenceKey, T> eventMap;
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

    public void append(PartitionedSequenceKey key, T event) {
        eventMap.set(key, event);
    }


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
     * @return a domain object reflecting all Events
     */
    public D materialize(D startingWith, String keyValue, int count, long upToTimestamp) {
        List<SourcedEvent<D,K>> events = getEventsFor(keyValue, count, upToTimestamp);
        return materialize(startingWith, events);
    }

    public D materialize(D startingWith, String keyValue) {
        return materialize(startingWith, keyValue, Integer.MAX_VALUE, Long.MAX_VALUE);
    }
    public D materialize(D startingWith, String keyValue, int count) {
        return materialize(startingWith, keyValue, count, Long.MAX_VALUE);

    }
    public D materialize (D startingWith, String keyValue, long upToTimestamp) {
        return materialize(startingWith, keyValue, Integer.MAX_VALUE, upToTimestamp);
    }

    private List<SourcedEvent<D,K>> getEventsFor(String keyValue, int count, long upToTimestamp) {
        initSqlService();

//        String query = "select * from " + eventMapName + " WHERE CAST(\"key\" AS VARCHAR) = '" + keyValue + "' ORDER BY __key";
//        SqlStatement statement = new SqlStatement(query);
//        System.out.println(statement);
//        SqlResult result = sqlService.execute(statement);

        String SELECT_TEMPLATE = "select * from ? WHERE CAST(\"key\" AS VARCHAR) = '?' ORDER BY __key";
        SqlStatement statement2 = new SqlStatement(SELECT_TEMPLATE)
                .setParameters(List.of(eventMapName, keyValue));
        System.out.println(statement2);
        SqlResult result = sqlService.execute(statement2);

        //SqlResult result = sqlService.execute(SELECT_TEMPLATE, List.of(eventMapName, keyValue));

        Iterator<SqlRow> iter = result.iterator();
        List<SourcedEvent<D,K>> events = new ArrayList<>();
        while (iter.hasNext()) {
            SqlRow row = iter.next();
            String eventClass = row.getObject("eventClass");
            try {
                Class<? extends SourcedEvent<D,K>> k = (Class<? extends SourcedEvent<D, K>>) Class.forName(eventClass);
                Constructor c = k.getConstructor(SqlRow.class);
                SourcedEvent<D,K> event = (SourcedEvent) c.newInstance(row);
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

    private long getEventCountFor(String keyName, String keyValue) {
        initSqlService();
        String query = "select COUNT(*) as event_count from " + eventMapName + " WHERE CAST(\"" +
                keyName + "\" AS VARCHAR) = '" + keyValue + "'";
        SqlStatement statement = new SqlStatement(query);
        System.out.println("Query: " + statement);
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
    private void deleteOldestEvents(String keyValue, int count) {
        initSqlService();
        String query = "select __key as psk from " + eventMapName + " WHERE CAST(\"" +
                "key\" AS VARCHAR) = '" + keyValue + "' ORDER BY psk";
        SqlStatement statement = new SqlStatement(query);
        System.out.println("Query: " + statement);
        SqlResult result = sqlService.execute(statement);
        Iterator<SqlRow> iter = result.iterator();
        long maxSequenceSeen = 0;
        int index = 0;
        while (iter.hasNext()) {
            SqlRow row = iter.next();
            PartitionedSequenceKey psk = row.getObject("psk");
            if (psk.getSequence() == 0) {
                System.out.println("Skipping compaction record when deleting old events");
                continue;
            }
            maxSequenceSeen = psk.getSequence();
            index++;
            SourcedEvent removed = eventMap.remove(psk);
            if (index >= count)
                break;
        }
    }

    public int compact(EventStoreCompactionEvent compactionEvent, D domainObject, String keyValue, double compressionFactor) {
        if (compressionFactor >= 1) {
            throw new IllegalArgumentException("Compression Factor must be < 1");
        }
        long eventCount = getEventCountFor("key", keyValue);
        int removalCount = (int) (eventCount * compressionFactor);
        System.out.println("Current count " + eventCount + " count to remove " + removalCount);
        if (removalCount < 1) return 0;
        List<SourcedEvent<D,K>> eventsToSummarize = getEventsFor(keyValue,
                removalCount, Long.MAX_VALUE);
        domainObject = materialize(domainObject, eventsToSummarize);
        PartitionedSequenceKey psk = new PartitionedSequenceKey(0, keyValue);
         compactionEvent.initFromDomainObject(domainObject);
        // Since compaction event isn't processed thru pending -> pipeline, it doesn't get timestamp set
        ((SourcedEvent)compactionEvent).setTimestamp(System.currentTimeMillis());
        //compactionEvent.writeAsCheckpoint(domainObject, psk);
        append(psk, (T) compactionEvent);
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
}
