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

package org.hazelcast.eventsourcing.event;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
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
import org.hazelcast.eventsourcing.testobjects.OpenAccountEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class OpenAccountTest {

    static EventSourcingController<Account, String, AccountEvent> controller;
    static SubscriptionManager<AccountEvent> submgr;
    static HazelcastInstance hazelcast;
    static boolean USE_IMAP_SUB_MGR = true;

    @BeforeAll
    static void init() {
        // Create Hazelcast structures
        Config config = new XmlConfigBuilder().build();
        config = EventSourcingController.addRequiredConfigItems(config);
        hazelcast = Hazelcast.newHazelcastInstance(config);
        controller = EventSourcingController.<Account,String,AccountEvent>newBuilder(hazelcast, "account")
                .build();

        // Create subscription manager, register it
        if (USE_IMAP_SUB_MGR)
            submgr = new IMapSubMgr<>("AccountEvent");
        else
            submgr = new ReliableTopicSubMgr<>();
        SubscriptionManager.register(hazelcast, OpenAccountEvent.class, submgr);
    }

    @AfterAll
    static void cleanUp() {
        hazelcast.shutdown();
    }

    @BeforeEach
    void setUp() {}

    @AfterEach
    void tearDown() {}

    @Test
    void verifyInitialBalance() throws ExecutionException, InterruptedException {
        AccountConsumer consumer = new AccountConsumer();
        submgr.subscribe(OpenAccountEvent.class.getCanonicalName(), consumer);

        // OpenAccountEvent is a SourcedEvent
        OpenAccountEvent open = new OpenAccountEvent("12345", "Bob", BigDecimal.valueOf(777.22));
        Future<CompletionInfo> completion = controller.handleEvent(open, null);
        CompletionInfo info = completion.get();
        Assertions.assertEquals(CompletionInfo.Status.COMPLETED_OK, info.status);

        // Get a materialized view that reflects the event
        EventStore<Account, String, AccountEvent> es = controller.getEventStore();
        Account a = es.materialize(new Account(), "12345");

        Assertions.assertEquals(BigDecimal.valueOf(777.22), a.getBalance());
        Assertions.assertEquals(1, consumer.getEventCount());
    }
}
