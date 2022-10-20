package org.hazelcast.eventsourcing.pubsub;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.hazelcast.eventsourcing.event.SourcedEvent;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

/**
 *
 * @param <E> Event class for which subscriptions are managed
 */
public abstract class SubscriptionManager<E> implements Serializable {

    // Optionally pass these to subscribe() method (UNIMPLEMENTED)
    public enum STARTING { FROM_NEXT, FROM_BEGINNING, FROM_OFFSET } // FROM_TIMESTAMP also?

    private static HazelcastInstance hazelcast = null;
    private static IMap<Class, List<SubscriptionManager>> event2submgr;
    // Need a way to identify different subscription managers that may be of the same subtype even
    // in serialized form where other identity checks may indicate not equal.
    private UUID uuid;
    private static final Logger logger = Logger.getLogger(SubscriptionManager.class.getName());


    protected SubscriptionManager() {
        this.uuid = UUID.randomUUID();
    }

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
        logger.info("Registered " + impl.getClass().getCanonicalName() + " for " + eventClass.getCanonicalName());
    }

    public static void unregister(HazelcastInstance hazelcast, final Class<? extends SourcedEvent> eventClass, SubscriptionManager impl) {
        if (SubscriptionManager.hazelcast == null) {
            SubscriptionManager.hazelcast = hazelcast;
            event2submgr = hazelcast.getMap("EventClass2SubMgr");
        }

        List<SubscriptionManager> managersForEvent = event2submgr.get(eventClass);
        if (managersForEvent == null) {
            logger.warning("Failed to unregister " + impl.getClass().getCanonicalName() + " for " +
                    eventClass.getCanonicalName() + ", no managers are registered for this event");
            return;
        }

        Iterator<SubscriptionManager> iter = managersForEvent.iterator();
        boolean matched = false;
        while (iter.hasNext()) {
            SubscriptionManager sm = iter.next();
            if (sm.uuid.equals(impl.uuid)) {
                matched = true;
                iter.remove();
                logger.info("Unregistered " + impl.getClass().getCanonicalName() + " for " + eventClass.getCanonicalName());
                break;
            }
        }
        if (!matched) {
            logger.warning("Failed to unregister " + impl.getClass().getCanonicalName() + " for " +
                    eventClass.getCanonicalName() + ", not registered");
        }
    }

    public static List<SubscriptionManager> getSubscriptionManagers(Class<? extends SourcedEvent> eventClass) {
        if (SubscriptionManager.event2submgr == null) {
            return Collections.emptyList();
        }
        return event2submgr.get(eventClass);
    }

    public abstract void subscribe(String eventName, Consumer c);
    public abstract void unsubscribe(String eventName, Consumer c);
    protected abstract void publish(String topicName, E event);
    public static <E extends SourcedEvent> void publish(E event) {
        Class<? extends SourcedEvent> eventClass = event.getClass();
        List<SubscriptionManager> mgrs = getSubscriptionManagers(eventClass);
        if (mgrs == null) {
            System.out.println("NOT PUBLISHING " + event + " because no subscription manager has registered for it:");
        }
        for (SubscriptionManager manager : mgrs) {
            manager.publish(eventClass.getCanonicalName(), event);
        }
    }

    // Implementing subclasses will likely store their event-to-consumer maps in Hazelcast
    protected HazelcastInstance getHazelcastInstance() {
        return hazelcast;
    }
}
