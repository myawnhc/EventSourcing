package org.hazelcast.eventsourcing.eventstore;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlStatement;
import org.hazelcast.eventsourcing.event.DomainObject;
import org.hazelcast.eventsourcing.event.SourcedEvent;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

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
 *
 *  T may be unneeded now that EntryEvent record type has been introduced
 */
public class EventStore<D extends DomainObject<K>, K, T extends SourcedEvent<D,K>>
                            implements Serializable, HazelcastInstanceAware {

    transient protected HazelcastInstance hazelcast;

    protected String eventMapName;
    transient protected IMap<Long, T> eventMap;
    //protected IAtomicLong sequenceProvider;
    transient protected SqlService sqlService;

    private String mapping_template = "CREATE MAPPING IF NOT EXISTS \"?\"\n" +
            "TYPE IMap\n" +
            "OPTIONS (\n" +
            "  'keyFormat' = 'java',\n" +
            "  'keyJavaClass' = 'java.lang.Long',\n" +
            "  'valueFormat' = 'java',\n" +
            "  'valueJavaClass' = 'org.hazelcast.eventsourcing.event.SourcedEvent'\n" +
            ")";

    public EventStore(String mapName, String keyName, Supplier<? extends D> domainObjConstructor, HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
        this.eventMapName = mapName;
        this.eventMap = hazelcast.getMap(mapName);
//        this.sequenceProvider = hazelcast.getCPSubsystem().getAtomicLong(mapName);
        //this.domainObjectConstructor = (SerializableSupplier<? extends D>) domainObjConstructor;
        // TODO: keyName would be used to ensure we have a proper index on the map, but we don't have sufficient
        // flexibility in dynamic configuration to do this on-the-fly at this time.
    }

    public void append(long sequence, T event) {
        SourcedEvent previous = eventMap.put(sequence, event);
        if (previous != null) {
            System.out.println("Sequence collision: " + event + " vs " + previous);
        }
        // Size reported here is not reliable!
        //System.out.println("EventStore.append " + event + " size is now " + eventMap.size());
    }

    // MSF functionality not yet implemented in ESF:
    /** Materialize a domain object from the event store.  In normal operation this isn't
     * used as we always keep an up-to-date materialized view, but in a recovery
     * scenario where the in-memory copy is lost this will rebuild it.
     *
     * param predicate A SQL format 'where' clause that selects the desired objects
     *                  from the event store.  Normally this would be in the form
     *                  {keyname}={value}, but for special cases it could specify
     *                  and condition.  Conditions that cause events for multiple
     *                  keys to be returned are not likely to produce useful results.
     * @return a domain object reflecting all Events
     */
    public D materialize(D startingWith, String keyValue) {
        D materializedObject = startingWith;
        List<SourcedEvent<D,K>> events = getEventsFor("key", keyValue);
        for (SourcedEvent<D,K> event: events) {
            materializedObject = event.apply(materializedObject);
        }
        return materializedObject;
    }
//    public D materialize(String predicate) {
//        // TODO: rework to use SQL rather than predicate API
//        // TODO: what parameterization is missing that requires cast of event?
//        D materializedObject = domainObjectConstructor.get();
//        List<Long> keys = new ArrayList(eventMap.keySet(Predicates.sql(predicate)));
//        Collections.sort(keys);
//        for (Long sequence : keys) {
//            EventEntry eventEntry = eventMap.get(sequence);
//            T event = (T) eventEntry.event(); // SourcedEvent
//            event.apply(materializedObject);
//        }
//        return materializedObject;
//    }

    /** Not sure this will remain public .. might have variations of materialize that
     * pass different criteria into here as user shouldn't have to be passing around
     * a SqlRow iterator
     *
     * @param keyName
     * @param keyValue
     * @return
     */
    public List<SourcedEvent<D,K>> getEventsFor(String keyName, String keyValue) {
        if (sqlService == null) {
            sqlService = hazelcast.getSql();
            // CREATE MAPPING doesn't support dynamic parameters, so we do the
            // substitution here.
            mapping_template = mapping_template.replaceAll("\\?", eventMapName);
            sqlService.execute(mapping_template);
        }
        // Having difficulty with parameter substitution in SqlStatement so just build
        // our own query string
        String all = "select * from " + eventMapName;
        String query = "select * from " + eventMapName + " WHERE CAST(\"" +
                keyName + "\" AS VARCHAR) = '" + keyValue + "' ORDER BY __key";

        SqlStatement statement = new SqlStatement(query);
        //statement.setParameters(List.of(eventMapName, keyName, keyValue));
        System.out.println("Query: " + statement);
        SqlResult result = sqlService.execute(statement);
        Iterator<SqlRow> iter = result.iterator();
        List<SourcedEvent<D,K>> events = new ArrayList<>();
        while (iter.hasNext()) {
            SqlRow row = iter.next();
            String eventClass = row.getObject("eventClass");
            try {
                Class<? extends SourcedEvent<D,K>> k = (Class<? extends SourcedEvent<D, K>>) Class.forName(eventClass);
                Constructor c = k.getConstructor(SqlRow.class);
                SourcedEvent<D,K> event = (SourcedEvent) c.newInstance(row);
                //System.out.println("Event created from SQL " + event);
                events.add(event);
            } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException |
                     InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
        return events;
    }

//    public void compact(String predicate, float compressionPercentage) {
//        // Note this shares a lot of code with materialize, should eventually
//        // refactor to eliminate duplication
//        D compressedData = domainObjectConstructor.get();
//        List<Long> keys = new ArrayList(eventMap.keySet(Predicates.sql(predicate)));
//        Collections.sort(keys);
//        int entriesToCompress = (int) (keys.size() * compressionPercentage);
//        System.out.println("Will compress " + entriesToCompress + " of " + keys.size() + " entries");
//        long sequenceOfLastAppliedEvent = -1;
//        for (int i=0; i<entriesToCompress; i++) {
//            T accountEvent = eventMap.get(keys.get(i));
//            accountEvent.apply(compressedData);
//            eventMap.remove(accountEvent);
//            sequenceOfLastAppliedEvent = keys.get(i);
//        }
//        // Now write the summarized object back into the slot of the last-compressed entry
//        T checkpointEvent = (T) writeAsCheckpoint(compressedData, sequenceOfLastAppliedEvent);
//        eventMap.put(sequenceOfLastAppliedEvent, checkpointEvent);
//    }
//
//    abstract public SequencedEvent writeAsCheckpoint(D domainObject, long sequence);

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        // Called on member side as part of pipeline initialization, we have to
        // re-init stuff set on the client side
        this.hazelcast = hazelcastInstance;
        this.eventMap = hazelcast.getMap(eventMapName);
    }
}
