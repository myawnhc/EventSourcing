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
import org.hazelcast.eventsourcing.pubsub.impl.IMapSubMgr;
import org.hazelcast.eventsourcing.testobjects.Account;
import org.hazelcast.eventsourcing.testobjects.AccountCompactionEvent;
import org.hazelcast.eventsourcing.testobjects.AccountEvent;
import org.hazelcast.eventsourcing.testobjects.AccountHydrationFactory;
import org.hazelcast.eventsourcing.testobjects.AccountMarkerEvent;
import org.hazelcast.eventsourcing.testobjects.BalanceChangeEvent;
import org.hazelcast.eventsourcing.testobjects.OpenAccountEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

public class TransactionsTestBase {

    static EventSourcingController<Account, String, AccountEvent> controller;
    static SubscriptionManager<AccountEvent> submgr;
    static HazelcastInstance hazelcast;

    public static final int TEST_EVENT_COUNT = 1_000;
    public static final String TEST_ACCOUNT = "67890";
    static List<BalanceChangeEvent> testEvents;

    @BeforeAll
    static void init() {
        hazelcast = Hazelcast.newHazelcastInstance();
        controller = EventSourcingController.<Account,String,AccountEvent>newBuilder(hazelcast, "account")
                .hydrationFactory(new AccountHydrationFactory())
                .build();

        testEvents = new ArrayList<>();
        for (int i=0; i<TEST_EVENT_COUNT; i++) {
            // Generate test events
            String eventName;
            int eventType = (int) (Math.random() * 100);
            BigDecimal amount = BigDecimal.valueOf(Math.random() * 1000).setScale(2, RoundingMode.HALF_UP);
            if (eventType < 2) {
                eventName = "Fee";
                amount = amount.multiply(BigDecimal.valueOf(-1));
            } else if (eventType < 5) {
                eventName = "Interest";
            } else if (eventType < 15) {
                eventName = "Deposit";
            } else if (eventType < 25) {
                eventName = "Withdrawal";
                amount = amount.multiply(BigDecimal.valueOf(-1));
            } else if (eventType < 50) {
                eventName = "Check";
                amount = amount.multiply(BigDecimal.valueOf(-1));
            } else {
                eventName = "Electronic Payment";
                amount = amount.multiply(BigDecimal.valueOf(-1));
            }
            BalanceChangeEvent event = new BalanceChangeEvent(TEST_ACCOUNT, eventName, amount);
            testEvents.add(event);
        }
    }

    @AfterAll
    static void cleanUp() {
        controller.shutdown();
        hazelcast.shutdown();
    }

    private static int testNumber = 0;

    @BeforeEach
    void setUp() {
        // Create subscription manager, register it
        submgr = new IMapSubMgr<>("AccountEvent");
        SubscriptionManager.register(hazelcast, OpenAccountEvent.QUAL_EVENT_NAME, submgr);
        SubscriptionManager.register(hazelcast, BalanceChangeEvent.QUAL_EVENT_NAME, submgr);
        SubscriptionManager.register(hazelcast, AccountMarkerEvent.QUAL_EVENT_NAME, submgr);
        SubscriptionManager.register(hazelcast, AccountCompactionEvent.QUAL_EVENT_NAME, submgr);
    }

    @AfterEach
    void tearDown() {
        SubscriptionManager.unregister(hazelcast, OpenAccountEvent.QUAL_EVENT_NAME, submgr);
        SubscriptionManager.unregister(hazelcast, BalanceChangeEvent.QUAL_EVENT_NAME, submgr);
        SubscriptionManager.unregister(hazelcast, AccountMarkerEvent.QUAL_EVENT_NAME, submgr);
        SubscriptionManager.unregister(hazelcast, AccountCompactionEvent.QUAL_EVENT_NAME, submgr);
        controller.getEventStore().clearData();
    }
}
