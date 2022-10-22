package org.hazelcast.eventsourcing.pubsub.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.ReliableMessageListener;
import org.hazelcast.eventsourcing.pubsub.Consumer;
import org.hazelcast.eventsourcing.pubsub.SubscriptionManager;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

public class ReliableTopicSubMgr<E> extends SubscriptionManager<E> {

    // Is this safely kept locally, or does it need to be an IMap?
    Map<SMKey<E>, UUID> subscriberMap = new HashMap<>();
    private static final Logger logger = Logger.getLogger(ReliableTopicSubMgr.class.getName());

    // Could use a record for this but would require JDK version > 11 (max supported by Viridian now)
    private static class SMKey<E> {
        String eventName;
        Consumer<E> consumer;
        public SMKey(String event, Consumer<E> c) {
            this.consumer = c;
            this.eventName = event;
        }
        @Override
        public boolean equals(Object o) {
            if (o instanceof ReliableTopicSubMgr.SMKey) {
                SMKey<E> other = (SMKey<E>) o;
                if (!other.eventName.equals(eventName))
                    return false;
                return other.consumer == consumer;
            }
            return false;
        }
        @Override
        public int hashCode() {
            return consumer.hashCode() + eventName.hashCode();
        }
    }

    class ReliableListener implements ReliableMessageListener<E> {

        private final Consumer<E> consumer;
        private long currentSequence;

        public ReliableListener(Consumer<E> c, long offset) {
            this.consumer = c;
            this.currentSequence = offset;
        }

        @Override
        public long retrieveInitialSequence() {
            return currentSequence;
        }

        @Override
        public void storeSequence(long l) {
            currentSequence = l;
        }

        @Override
        public boolean isLossTolerant() {
            return false;
        }

        @Override
        public boolean isTerminal(Throwable throwable) {
            return false;
        }

        @Override
        public void onMessage(Message<E> message) {
            try {
                consumer.onEvent(message.getMessageObject());
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    @Override
    public void subscribe(String eventName, Consumer<E> c) {
        subscribe(eventName, c, 0);
    }

    @Override
    public void subscribe(String eventName, Consumer<E> c, long fromOffset) {
        HazelcastInstance hz = super.getHazelcastInstance();
        ITopic<E> topic = hz.getReliableTopic(eventName);
        UUID uuid = topic.addMessageListener(new ReliableListener(c, fromOffset));
        SMKey<E> key = new SMKey<>(eventName, c);
        subscriberMap.put(key, uuid);
        logger.info("Consumer " + c + " subscribed to " + eventName);
    }

    @Override
    public void unsubscribe(String eventName, Consumer<E> c) {
        HazelcastInstance hz = super.getHazelcastInstance();
        ITopic<E> topic = hz.getReliableTopic(eventName);
        SMKey<E> key = new SMKey<>(eventName, c);
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
        ITopic<E> topic = hz.getReliableTopic(topicName);
        topic.publish(event);
    }

    // used for unit tests only
    public void clearData(String topicName) {
        ITopic<E> topic = super.getHazelcastInstance().getReliableTopic(topicName);
        topic.destroy();
    }
}
