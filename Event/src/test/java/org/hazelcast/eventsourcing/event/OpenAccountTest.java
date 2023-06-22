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
import org.hazelcast.eventsourcing.eventstore.EventStore;
import org.hazelcast.eventsourcing.pubsub.SubscriptionManager;
import org.hazelcast.eventsourcing.pubsub.impl.IMapSubMgr;
import org.hazelcast.eventsourcing.pubsub.impl.ReliableTopicSubMgr;
import org.hazelcast.eventsourcing.sync.CompletionInfo;
import org.hazelcast.eventsourcing.testobjects.Account;
import org.hazelcast.eventsourcing.testobjects.AccountConsumer;
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class OpenAccountTest {

    static EventSourcingController<Account, String, AccountEvent> controller;
    static SubscriptionManager<AccountEvent> submgr;
    static HazelcastInstance hazelcast;

    @BeforeAll
    static void init() {
        // Create Hazelcast structures
        hazelcast = Hazelcast.newHazelcastInstance();
        controller = EventSourcingController.<Account,String,AccountEvent>newBuilder(hazelcast, "account")
                .hydrationFactory(new AccountHydrationFactory())
                .build();

        // Create subscription manager, register it
        submgr = new IMapSubMgr<>("AccountEvent");
        SubscriptionManager.register(hazelcast, OpenAccountEvent.QUAL_EVENT_NAME, submgr);
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
    void verifyInitialBalance() throws ExecutionException, InterruptedException {
        AccountConsumer consumer = new AccountConsumer();
        submgr.subscribe(OpenAccountEvent.QUAL_EVENT_NAME, consumer);

        // OpenAccountEvent is a SourcedEvent
        OpenAccountEvent open = new OpenAccountEvent("12345", "Bob", BigDecimal.valueOf(777.22));
        CompletableFuture<CompletionInfo> completion = controller.handleEvent(open, UUID.randomUUID());
        CompletionInfo info = completion.get();
        Assertions.assertTrue(info.getEvent() instanceof OpenAccountEvent);
        Assertions.assertEquals(CompletionInfo.Status.COMPLETED_OK, info.status);

        // Get a materialized view that reflects the event
        EventStore<Account, String, AccountEvent> es = controller.getEventStore();
        Account a = es.materialize(new Account(), "12345");

        Assertions.assertEquals(BigDecimal.valueOf(777.22), a.getBalance());
        Assertions.assertEquals(1, consumer.getEventCount());

        submgr.unsubscribe(OpenAccountEvent.QUAL_EVENT_NAME, consumer);
    }
}
