package org.hazelcast.eventsourcing.pubsub.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.hazelcast.eventsourcing.pubsub.Consumer;
import org.hazelcast.eventsourcing.pubsub.SubscriptionManager;

import java.util.ArrayList;
import java.util.List;

public class ReliableTopicSubMgr<E> extends SubscriptionManager<E> {

    private IMap<String, List<Consumer<E>>> subscribersByEventName;

    @Override
    public void subscribe(String eventName, Consumer c) {
        if (subscribersByEventName == null) {
            HazelcastInstance hz = super.getHazelcastInstance();
            subscribersByEventName = hz.getMap("RT_EventClass2Consumers");
        }
        List<Consumer<E>> clist = subscribersByEventName.get(eventName);
        if (clist == null)
            clist = new ArrayList<>();
        clist.add(c);
        subscribersByEventName.put(eventName, clist);
    }

    @Override
    public void publish(String eventName, E event) {
        if (subscribersByEventName == null) {
            HazelcastInstance hz = super.getHazelcastInstance();
            subscribersByEventName = hz.getMap("RT_EventClass2Consumers");
        }
        List<Consumer<E>> clist = subscribersByEventName.get(eventName);
        if (clist == null)
            System.out.println("Warning: No subscribers to event " + eventName);
        else {
            for (Consumer<E> c : clist) {
                c.onEvent(event);
            }
        }
    }
}
