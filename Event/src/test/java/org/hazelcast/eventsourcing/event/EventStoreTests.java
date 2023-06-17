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

package org.hazelcast.eventsourcing.event;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.hazelcast.eventsourcing.EventSourcingController;
import org.hazelcast.eventsourcing.pubsub.SubscriptionManager;
import org.hazelcast.eventsourcing.testobjects.Account;
import org.hazelcast.eventsourcing.testobjects.AccountEvent;
import org.hazelcast.eventsourcing.testobjects.AccountHydrationFactory;
import org.hazelcast.eventsourcing.testobjects.OpenAccountEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Set;

public class EventStoreTests {

    static EventSourcingController<Account, String, AccountEvent> controller;
    static HazelcastInstance hazelcast;

    @BeforeAll
    static void init() {
        // Create Hazelcast structures
        hazelcast = Hazelcast.newHazelcastInstance();
        controller = EventSourcingController.<Account,String,AccountEvent>newBuilder(hazelcast, "account")
                .hydrationFactory(new AccountHydrationFactory())
                .build();
    }

    @AfterAll
    static void cleanUp() {
        controller.shutdown(); // Cancels the EventSourcingPipeline job
        hazelcast.shutdown();  // could move this into controller shutdown
    }

    @BeforeEach
    void setUp() {}

    @AfterEach
    void tearDown() {}

    @Test
    void verifyAppend() {
        // OpenAccountEvent is a SourcedEvent
        OpenAccountEvent open1 = new OpenAccountEvent("11111", "Bob", BigDecimal.valueOf(111.22));
        PartitionedSequenceKey<String> key1 = controller.handleEvent(open1);

        OpenAccountEvent open2 = new OpenAccountEvent("22222", "Rick", BigDecimal.valueOf(333.44));
        PartitionedSequenceKey<String> key2 = controller.handleEvent(open2);

        OpenAccountEvent open3 = new OpenAccountEvent("33333", "Jim", BigDecimal.valueOf(555.66));
        PartitionedSequenceKey<String> key3 = controller.handleEvent(open3);

        // Allow pipeline to finish processing events
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        int storeSize = controller.getEventStore().getSize();
        Assertions.assertEquals(3, storeSize);

        Set<PartitionedSequenceKey<String>> keySet = controller.getEventStore().getKeys();
        Assertions.assertEquals(3, keySet.size());
        Assertions.assertTrue(keySet.contains(key1));
        Assertions.assertTrue(keySet.contains(key2));
        Assertions.assertTrue(keySet.contains(key3));

        controller.getEventStore().clearData();
    }
}
