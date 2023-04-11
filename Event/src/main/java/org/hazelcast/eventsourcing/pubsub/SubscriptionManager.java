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

package org.hazelcast.eventsourcing.pubsub;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.map.IMap;
import org.hazelcast.eventsourcing.event.PartitionedSequenceKey;
import org.hazelcast.eventsourcing.event.SourcedEvent;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

/** Manage subscriptions between services for event notifications.
 *
 * This abstract base class is used to support multiple messaging frameworks that
 * might be in use concurrently since different services may have different
 * technology stacks .. so there may be services using Kafka, gRPC, REST, ITopic,
 * JMS, etc.
 *
 * At this point, only Hazelcast's Reliable Topic has been implemented within the
 * example framework, others may be added over time.  It's possible (perhaps even
 * likely) that the pub-sub APIs may need to evolve to support multiple protocols
 * so the APIs of this part of the framework should be considered unstable.
 *
 * @param <E> Event class for which subscriptions are managed
 */
public abstract class SubscriptionManager<E extends SourcedEvent> implements Serializable {

    // Optionally pass these to subscribe() method (UNIMPLEMENTED)
    public enum STARTING { FROM_NEXT, FROM_BEGINNING, FROM_OFFSET } // FROM_TIMESTAMP also?

    private static HazelcastInstance hazelcast = null;
    private static IMap<Class<? extends SourcedEvent>, List<SubscriptionManager>> event2submgr;
    // Need a way to identify different subscription managers that may be of the same subtype even
    // in serialized form where other identity checks may indicate not equal.
    private final UUID uuid;
    private static final Logger logger = Logger.getLogger(SubscriptionManager.class.getName());

    protected SubscriptionManager() {
        this.uuid = UUID.randomUUID();
    }

    /** Register a subscription manager for an event class.  Subscribing to events is a
     * two-step process, first the Subscription Manager subclass must be registered to
     * handle the events, and then the consumer of the events must subscribe.
     * @param hazelcast Hazelcast Instance handling the subscriptions
     * @param eventClass event class for which subscription manager should be registered
     * @param impl implementation of SubscriptionManager to register
     */
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

    /** Unegister a subscription manager for an event class.
     *
     * @param hazelcast Hazelcast Instance handling the subscriptions
     * @param eventClass event class for which subscription manager should be registered
     * @param impl implementation of SubscriptionManager to register
     */
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

    /** Returns the SubscriptionManager implementations registered for an event */
    private static List<SubscriptionManager> getSubscriptionManagers(Class<? extends SourcedEvent> eventClass) {
        if (SubscriptionManager.event2submgr == null) {
            return Collections.emptyList();
        }
        return event2submgr.get(eventClass);
    }

    /** Subscribe the provided consumer to the named event */
    public abstract void subscribe(String eventName, Consumer<E> c);
    /** Subscribe the provided consumer to the named event, beginning with the
     * specified message sequence number.  Typically used to resume receiving
     * messages after an outage; messaging implementations that provide
     * storage can resend messages that might have been missed.
     */
    public abstract void subscribe(String eventName, Consumer<E> c, long fromOffset);
    /** Unsubscribe the provided consumer from the named event */
    public abstract void unsubscribe(String eventName, Consumer<E> c);

    /** Publish a message to all subscribers.  This method is implemented
     * by subscription manager implementations; the static method that
     * doesn't require a topic name is the one applications will call
     * to publish events.
     *
     * @param topicName fully qualified class name of the event class
     * @param event event to be published
     */
    protected abstract void publish(String topicName, E event);
    /** Publish a message to all subscribers
     * @param event event to be published
     */
    public static <E extends SourcedEvent> void publish(E event) {
        Class<? extends SourcedEvent> eventClass = event.getClass();
        List<SubscriptionManager> mgrs = getSubscriptionManagers(eventClass);
        if (mgrs == null) {
            System.out.println("NOT PUBLISHING " + event + " because no subscription manager has registered for it");
        } else {
            // Because we have different subscription managers per service, seems unnecessary to
            // use full canonical name for events, should be no collisions.
            for (SubscriptionManager manager : mgrs) {
                manager.publish(eventClass.getSimpleName(), event);
            }
        }
    }

    // Optionally allow use as a Jet endpoint
    public StreamSource<Map.Entry<PartitionedSequenceKey, E>> getStreamSource(String eventName) { return null; }
    public Sink<E> getSink() { return null; }

    // Implementing subclasses will likely store their event-to-consumer maps in Hazelcast
    protected HazelcastInstance getHazelcastInstance() {
        return hazelcast;
    }
}
