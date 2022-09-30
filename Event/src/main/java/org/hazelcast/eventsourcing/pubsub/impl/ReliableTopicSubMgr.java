package org.hazelcast.eventsourcing.pubsub.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.topic.ITopic;
import org.hazelcast.eventsourcing.pubsub.Consumer;
import org.hazelcast.eventsourcing.pubsub.SubscriptionManager;

import java.util.List;

public class ReliableTopicSubMgr<E> extends SubscriptionManager<E> {

    private IMap<String, List<Consumer<E>>> subscribersByEventName;

    @Override
    public void subscribe(String eventName, Consumer c) {
        HazelcastInstance hz = super.getHazelcastInstance();
        ITopic topic = hz.getReliableTopic(eventName);
        System.out.println("Subscribing with consumer " + c);
        topic.addMessageListener(message -> {
            System.out.println("Delivering message to consumer " + c);
            try {
                c.onEvent(message.getMessageObject()); // Message cannot be cast to OpenAccountEvent ...
            } catch (Throwable t) {
                t.printStackTrace();
            }
        });
//        if (subscribersByEventName == null) {
//            HazelcastInstance hz = super.getHazelcastInstance();
//            subscribersByEventName = hz.getMap("RT_EventClass2Consumers");
//        }
//        List<Consumer<E>> clist = subscribersByEventName.get(eventName);
//        if (clist == null)
//            clist = new ArrayList<>();
//        clist.add(c);
//        subscribersByEventName.put(eventName, clist);
    }

    @Override
    public void publish(String eventName, E event) {
        HazelcastInstance hz = super.getHazelcastInstance();
        ITopic topic = hz.getReliableTopic(eventName);
        topic.publish(event);
//        if (subscribersByEventName == null) {
//            HazelcastInstance hz = super.getHazelcastInstance();
//            subscribersByEventName = hz.getMap("RT_EventClass2Consumers");
//        }
//        List<Consumer<E>> clist = subscribersByEventName.get(eventName);
//        if (clist == null)
//            System.out.println("Warning: No subscribers to event " + eventName);
//        else {
//            for (Consumer<E> c : clist) {
//                c.onEvent(event);
//            }
//        }
    }
}
