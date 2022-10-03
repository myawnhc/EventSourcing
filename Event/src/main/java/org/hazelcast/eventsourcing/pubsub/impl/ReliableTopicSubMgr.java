package org.hazelcast.eventsourcing.pubsub.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;
import org.hazelcast.eventsourcing.pubsub.Consumer;
import org.hazelcast.eventsourcing.pubsub.SubscriptionManager;

public class ReliableTopicSubMgr<E> extends SubscriptionManager<E> {

    @Override
    public void subscribe(String eventName, Consumer c) {
        HazelcastInstance hz = super.getHazelcastInstance();
        ITopic topic = hz.getReliableTopic(eventName);
        topic.addMessageListener(message -> {
            try {
                c.onEvent(message.getMessageObject());
            } catch (Throwable t) {
                t.printStackTrace();
            }
        });
    }

    @Override
    public void publish(String eventName, E event) {
        HazelcastInstance hz = super.getHazelcastInstance();
        ITopic topic = hz.getReliableTopic(eventName);
        topic.publish(event);
    }
}
