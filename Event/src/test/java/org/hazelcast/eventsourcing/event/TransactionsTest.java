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
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import org.hazelcast.eventsourcing.EventSourcingController;
import org.hazelcast.eventsourcing.eventstore.EventStore;
import org.hazelcast.eventsourcing.pubsub.SubscriptionManager;
import org.hazelcast.eventsourcing.pubsub.impl.IMapSubMgr;
import org.hazelcast.eventsourcing.sync.CompletionInfo;
import org.hazelcast.eventsourcing.testobjects.Account;
import org.hazelcast.eventsourcing.testobjects.AccountCompactionEvent;
import org.hazelcast.eventsourcing.testobjects.AccountConsumer;
import org.hazelcast.eventsourcing.testobjects.AccountEvent;
import org.hazelcast.eventsourcing.testobjects.AccountHydrationFactory;
import org.hazelcast.eventsourcing.testobjects.BalanceChangeEvent;
import org.hazelcast.eventsourcing.testobjects.OpenAccountEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class TransactionsTest {

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
    }

    @AfterEach
    void tearDown() {
        SubscriptionManager.unregister(hazelcast, OpenAccountEvent.QUAL_EVENT_NAME, submgr);
        SubscriptionManager.unregister(hazelcast, BalanceChangeEvent.QUAL_EVENT_NAME, submgr);
        controller.getEventStore().clearData();
    }

    @Test
    void verifyTransactions() {
        AccountConsumer consumer = new AccountConsumer();
        int offset = testNumber * TEST_EVENT_COUNT;
        submgr.subscribe(OpenAccountEvent.QUAL_EVENT_NAME, consumer, testNumber);
        submgr.subscribe(BalanceChangeEvent.QUAL_EVENT_NAME, consumer, offset);
        testNumber++;

        // Open the account
        OpenAccountEvent open = new OpenAccountEvent(TEST_ACCOUNT, "Test Account", BigDecimal.valueOf(100.00));
        System.out.println("Event 1 = " + open);

        controller.handleEvent(open, UUID.randomUUID());

        BigDecimal expectedBalance = BigDecimal.valueOf(100.00); // Set to initial balance

        // Throw transactions at the account
        System.out.println("Sending " + testEvents.size() + " randomized events");
        int expectedSequence = 2; // 1 will be the open event
        for (int i=0; i<testEvents.size(); i++) {
            BalanceChangeEvent event = testEvents.get(i);
            //System.out.println("Event " + (i+2) + " = " + event);
            //JSONObject jobj = new JSONObject(event.getPayload().getValue());
            expectedBalance = expectedBalance.add(event.getBalanceChange());
            CompletableFuture<CompletionInfo> psk = controller.handleEvent(testEvents.get(i), UUID.randomUUID());
            //System.out.println("Ack: " + psk);
            //Assertions.assertEquals(psk.sequence, expectedSequence++);
        }

        System.out.println("Waiting for pipeline to clear ...");
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Get a materialized view that reflects the event.  Will fail if events
        // are still making their way through the pipeline, hence the delay above.
        EventStore<Account, String, AccountEvent> es = controller.getEventStore();
        GenericRecord a = es.materialize(new Account().toGenericRecord(), TEST_ACCOUNT);

        Assertions.assertEquals(expectedBalance, a.getDecimal(Account.FIELD_BALANCE));
        Assertions.assertEquals(TEST_EVENT_COUNT+1, consumer.getEventCount());

        submgr.unsubscribe(OpenAccountEvent.QUAL_EVENT_NAME, consumer);
        submgr.unsubscribe(BalanceChangeEvent.QUAL_EVENT_NAME, consumer);
    }

    @Test
    /** Same as transactions test, except we do verification, compact 50%, then
     * verify again to ensure we get the same result.
     */
    void verifyCompaction() {
        AccountConsumer consumer = new AccountConsumer();
        int offset = testNumber * TEST_EVENT_COUNT;
        submgr.subscribe(OpenAccountEvent.QUAL_EVENT_NAME, consumer, testNumber);
        submgr.subscribe(BalanceChangeEvent.QUAL_EVENT_NAME, consumer, offset);
        testNumber++;

        // Open the account
        OpenAccountEvent open = new OpenAccountEvent(TEST_ACCOUNT, "Test Account", BigDecimal.valueOf(100.00));
        System.out.println("Event 1 = " + open);
        controller.handleEvent(open, UUID.randomUUID());

        BigDecimal expectedBalance = BigDecimal.valueOf(100.00); // Set to initial balance

        // Throw transactions at the account
        System.out.println("Sending " + testEvents.size() + " randomized events");
        for (int i=0; i<testEvents.size(); i++) {
            BalanceChangeEvent event = testEvents.get(i);
            //System.out.println("Event " + (i+2) + " = " + event);
            expectedBalance = expectedBalance.add(event.getBalanceChange());
            controller.handleEvent(testEvents.get(i), UUID.randomUUID());
        }

        System.out.println("Waiting for pipeline to clear ...");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


        // Get a materialized view that reflects the event
        EventStore<Account, String, AccountEvent> es = controller.getEventStore();
        GenericRecord a = es.materialize(new Account().toGenericRecord(), TEST_ACCOUNT);

        Assertions.assertEquals(expectedBalance, a.getDecimal(Account.FIELD_BALANCE), "Expected balance before compaction");
        Assertions.assertEquals(TEST_EVENT_COUNT+1, consumer.getEventCount(), "Event count before compaction wrong");

        es.compact(new AccountCompactionEvent(), new Account().toGenericRecord(), "67890", 0.5);
        GenericRecord a2 = es.materialize(new Account().toGenericRecord(), TEST_ACCOUNT);

        Assertions.assertEquals(expectedBalance, a2.getDecimal(Account.FIELD_BALANCE), "Expected balance after compaction");
        // TODO: don't want messaging count here, but actual count ... haven't exposed
        //  getEventsFor but might need to for this text.
        //Assertions.assertEquals(51, consumer.getEventCount());

        submgr.unsubscribe(OpenAccountEvent.QUAL_EVENT_NAME, consumer);
        submgr.unsubscribe(BalanceChangeEvent.QUAL_EVENT_NAME, consumer);
    }
}
