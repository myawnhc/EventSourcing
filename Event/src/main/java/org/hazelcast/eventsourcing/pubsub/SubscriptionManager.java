package org.hazelcast.eventsourcing.pubsub;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.hazelcast.eventsourcing.event.SourcedEvent;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 * @param <E> Event class for which subscriptions are managed
 */
public abstract class SubscriptionManager<E> implements Serializable {

    // Optionally pass these to subscribe() method (UNIMPLEMENTED)
    enum STARTING { FROM_NEXT, FROM_BEGINNING, FROM_OFFSET } // FROM_TIMESTAMP also?

   private static HazelcastInstance hazelcast = null;
   private static IMap<Class, List<SubscriptionManager>> event2submgr;

    public static void register(HazelcastInstance hazelcast, Class<? extends SourcedEvent> eventClass, SubscriptionManager impl) {
        if (SubscriptionManager.hazelcast == null) {
            SubscriptionManager.hazelcast = hazelcast;
            event2submgr = hazelcast.getMap("EventClass2SubMgr");
        }

        List<SubscriptionManager> managersForEvent = event2submgr.get(eventClass);
        if (managersForEvent == null)
                managersForEvent = new ArrayList<>();
        managersForEvent.add(impl);
        event2submgr.put(eventClass, managersForEvent);
        System.out.println("Registered " + impl.getClass().getCanonicalName() + " for " + eventClass.getCanonicalName());
    }

    public static List<SubscriptionManager> getSubscriptionManagers(Class<? extends SourcedEvent> eventClass) {
        if (SubscriptionManager.event2submgr == null) {
            return Collections.emptyList();
        }
        return event2submgr.get(eventClass);
    }

    public abstract void subscribe(String eventName, Consumer c);
    //public void publish(E event);
    public abstract void publish(String eventName, E event);

    // Implementing subclasses will likely store their event-to-consumer maps in Hazelcast
    protected HazelcastInstance getHazelcastInstance() {
        return hazelcast;
    }
}
