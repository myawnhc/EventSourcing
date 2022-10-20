package org.hazelcast.eventsourcing.pubsub.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;
import org.hazelcast.eventsourcing.pubsub.Consumer;
import org.hazelcast.eventsourcing.pubsub.SubscriptionManager;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

public class ReliableTopicSubMgr<E> extends SubscriptionManager<E> {

    // Is this safely kept locally, or does it need to be an IMap?
    Map<SMKey, UUID> subscriberMap = new HashMap<>();
    private static final Logger logger = Logger.getLogger(ReliableTopicSubMgr.class.getName());

    // Could use a record for this but would require JDK version > 11 (max supported by Viridian now)
    private class SMKey {
        String eventName;
        Consumer consumer;
        public SMKey(String event, Consumer c) {
            this.consumer = c;
            this.eventName = event;
        }
        @Override
        public boolean equals(Object o) {
            if (o instanceof ReliableTopicSubMgr.SMKey) {
                SMKey other = (SMKey) o;
                if (!other.eventName.equals(eventName))
                    return false;
                if (!(other.consumer == consumer))
                    return false;
                return true;
            }
            return false;
        }
        @Override
        public int hashCode() {
            return consumer.hashCode() + eventName.hashCode();
        }
    }

    @Override
    public void subscribe(String eventName, Consumer c) {
        HazelcastInstance hz = super.getHazelcastInstance();
        ITopic topic = hz.getReliableTopic(eventName);
        UUID uuid = topic.addMessageListener(message -> {
            try {
                c.onEvent(message.getMessageObject());
            } catch (Throwable t) {
                t.printStackTrace();
            }
        });
        SMKey key = new SMKey(eventName, c);
        subscriberMap.put(key, uuid);
        logger.info("Consumer " + c + " subscribed to " + eventName);
    }

    @Override
    public void unsubscribe(String eventName, Consumer c) {
        HazelcastInstance hz = super.getHazelcastInstance();
        ITopic topic = hz.getReliableTopic(eventName);
        SMKey key = new SMKey(eventName, c);
        UUID subscriberUUID = subscriberMap.get(key);
        if (subscriberUUID == null) {
            logger.warning("unsubscribe request ignored, " + c + " is not a subscriber to " + eventName);
        } else {
            topic.removeMessageListener(subscriberUUID);
            subscriberMap.remove(key);
        }
    }

    @Override
    public void publish(String topicName, E event) {
        HazelcastInstance hz = super.getHazelcastInstance();
        ITopic topic = hz.getReliableTopic(topicName);
        topic.publish(event);
    }
}
