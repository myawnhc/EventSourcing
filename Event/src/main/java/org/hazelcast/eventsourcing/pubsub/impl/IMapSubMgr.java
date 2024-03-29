/*
 * Copyright 2022-2023 Hazelcast, Inc
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

package org.hazelcast.eventsourcing.pubsub.impl;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import org.hazelcast.eventsourcing.event.PartitionedSequenceKey;
import org.hazelcast.eventsourcing.event.SourcedEvent;
import org.hazelcast.eventsourcing.pubsub.Consumer;
import org.hazelcast.eventsourcing.pubsub.SubscriptionManager;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

public class IMapSubMgr <E extends SourcedEvent> extends SubscriptionManager<E> {
    // Is this safely kept locally, or does it need to be an IMap?
    Map<SMKey<E>, UUID> subscriberMap = new HashMap<>();
    private static final Logger logger = Logger.getLogger(IMapSubMgr.class.getName());

    // IMap not serializable and SubMgr is used in pipeline creation -- so leaving
//    // template mapping in place but removing actual journal declaration
//    //private IMap<PartitionedSequenceKey, E> eventJournal;
//    private String eventJournalName;
//
//    private String eventJournal_mapping_template = "CREATE MAPPING IF NOT EXISTS \"?\"\n" +
//            "TYPE IMap\n" +
//            "OPTIONS (\n" +
//            "  'keyFormat' = 'java',\n" +
//            "  'keyJavaClass' = 'org.hazelcast.eventsourcing.event.PartitionedSequenceKey',\n" +
//            "  'valueFormat' = 'java',\n" +
//            "  'valueJavaClass' = 'org.hazelcast.eventsourcing.sync.CompletionInfo'\n" +
//            ")";


    // Could use a record for this but would require JDK version > 11 (max supported by Viridian now)
    // Identical to implementation in ReliableTopicSubMgr - should it be in base class?
    private static class SMKey<E> {
        String eventName;
        Consumer<E> consumer;
        public SMKey(String event, Consumer<E> c) {
            this.consumer = c;
            this.eventName = event;
        }
        @Override
        public boolean equals(Object o) {
            if (o instanceof IMapSubMgr.SMKey) {
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

    public IMapSubMgr(String baseEventClassName) {
//        eventJournalName = mapNameFromEventName(baseEventClassName);
        //eventJournal = getHazelcastInstance().getMap(eventJournalName);
//        eventJournal_mapping_template = eventJournal_mapping_template.replaceAll("\\?", eventJournalName);
        // NO - Base class HZ not initialized when we construct this instance ..
        //getHazelcastInstance().getSql().execute(eventJournal_mapping_template);
    }

    private String mapNameFromEventName(String eventName) {
        return "JRN." + eventName;
    }

    @Override
    public void subscribe(String eventName, Consumer<E> c) {
        subscribe(eventName, c, 0);
    }

    @Override
    public void subscribe(String eventName, Consumer<E> c, long fromOffset) {
        HazelcastInstance hz = super.getHazelcastInstance();
        IMap<PartitionedSequenceKey, E> map = hz.getMap(mapNameFromEventName(eventName));
        UUID uuid = map.addEntryListener(new EntryAddedListener() {
            @Override
            public void entryAdded(EntryEvent entryEvent) {
                c.onEvent((E)entryEvent.getValue());
            }
        }, true); // true = return the value
        SMKey<E> key = new SMKey<>(eventName, c);
        subscriberMap.put(key, uuid);
        logger.info("Consumer " + c + " subscribed to " + eventName);
    }

    @Override
    public void unsubscribe(String eventName, Consumer<E> c) {
        HazelcastInstance hz = super.getHazelcastInstance();
        IMap<PartitionedSequenceKey,E> map = hz.getMap(mapNameFromEventName(eventName));
        SMKey<E> key = new SMKey<>(eventName, c);
        UUID subscriberUUID = subscriberMap.get(key);
        if (subscriberUUID == null) {
            logger.warning("unsubscribe request ignored, " + c + " is not a subscriber to " + eventName);
        } else {
            map.removeEntryListener(subscriberUUID);
            subscriberMap.remove(key);
        }
    }

    @Override
    public void publish(String eventName, E event) {
        HazelcastInstance hz = super.getHazelcastInstance();
        String mapName = mapNameFromEventName(eventName);
        IMap<PartitionedSequenceKey, E> map = hz.getMap(mapName);
        long sequence = hz.getPNCounter(eventName).getAndIncrement();
        PartitionedSequenceKey key = new PartitionedSequenceKey(sequence, event.getKey());
        //logger.info("IMapSubMgr publishes :" + eventName + ": " + key + " to " + mapName);
        map.put(key, event);
    }

    @Override
    public StreamSource<Map.Entry<PartitionedSequenceKey, E>> getStreamSource(String eventName) {
        String mapName = mapNameFromEventName(eventName);
        // There must be at least one subscriber for messages to be written to the map,
        // so add an empty subscriber.
        subscribe(eventName, eventMessage -> {
            // nop
        });
        logger.info("IMapSubMgr getStreamSource(" + eventName + ") subscribes to " + mapName);
        return Sources.mapJournal(mapName, JournalInitialPosition.START_FROM_OLDEST);
    }

    // used for unit tests only
    public void clearData(String eventName) {
        HazelcastInstance hz = super.getHazelcastInstance();
        String mapName = mapNameFromEventName(eventName);
        hz.getMap(mapName).clear();
    }
}
