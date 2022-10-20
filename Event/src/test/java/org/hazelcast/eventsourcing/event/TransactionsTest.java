package org.hazelcast.eventsourcing.event;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.org.json.JSONObject;
import org.hazelcast.eventsourcing.EventSourcingController;
import org.hazelcast.eventsourcing.eventstore.EventStore;
import org.hazelcast.eventsourcing.pubsub.SubscriptionManager;
import org.hazelcast.eventsourcing.pubsub.impl.ReliableTopicSubMgr;
import org.hazelcast.eventsourcing.testobjects.Account;
import org.hazelcast.eventsourcing.testobjects.AccountCompactionEvent;
import org.hazelcast.eventsourcing.testobjects.AccountConsumer;
import org.hazelcast.eventsourcing.testobjects.AccountEvent;
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

public class TransactionsTest {

    static EventSourcingController<Account, String, AccountEvent> controller;
    static SubscriptionManager<AccountEvent> submgr;
    static HazelcastInstance hazelcast;

    public static final int TEST_EVENT_COUNT = 10_000;
    public static final String TEST_ACCOUNT = "67890";
    static List<AccountEvent> testEvents;

    @BeforeAll
    static void init() {
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
            AccountEvent event = new BalanceChangeEvent(TEST_ACCOUNT, eventName, amount);
            testEvents.add(event);
        }
    }

    @AfterAll
    static void cleanUp() {}

    @BeforeEach
    void setUp() {
        hazelcast = Hazelcast.newHazelcastInstance();
        controller = EventSourcingController.newBuilder(hazelcast, "account")
                .build();

        // Create subscription manager, register it
        submgr = new ReliableTopicSubMgr<>();
        SubscriptionManager.register(hazelcast, OpenAccountEvent.class, submgr);
        SubscriptionManager.register(hazelcast, BalanceChangeEvent.class, submgr);
    }

    @AfterEach
    void tearDown() {
        SubscriptionManager.unregister(hazelcast, OpenAccountEvent.class, submgr);
        SubscriptionManager.unregister(hazelcast, BalanceChangeEvent.class, submgr);
        controller.shutdown();
        Hazelcast.shutdownAll();
        // Subsequent test will fail with HazelcastInstanceNotActive . let this one
        // fully clear before we start the next test
//        try {
//            Thread.sleep(15000);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
    }

    @Test
    void verifyTransactions() {
        AccountConsumer consumer = new AccountConsumer();
        submgr.subscribe(OpenAccountEvent.class.getCanonicalName(), consumer);
        submgr.subscribe(BalanceChangeEvent.class.getCanonicalName(), consumer);

        // Open the account
        OpenAccountEvent open = new OpenAccountEvent(TEST_ACCOUNT, "Test Account", BigDecimal.valueOf(100.00));
        System.out.println("Event 1 = " + open);

        controller.handleEvent(open);

        BigDecimal expectedBalance = BigDecimal.valueOf(100.00); // Set to initial balance

        // Throw transactions at the account
        System.out.println("Sending " + testEvents.size() + " randomized events");
        for (int i=0; i<testEvents.size(); i++) {
            AccountEvent event = testEvents.get(i);
            System.out.println("Event " + (i+2) + " = " + event);
            JSONObject jobj = new JSONObject(event.getPayload().getValue());
            expectedBalance = expectedBalance.add(jobj.getBigDecimal("balanceChange"));
            controller.handleEvent(testEvents.get(i));
        }

        // Get a materialized view that reflects the event
        EventStore<Account, String, AccountEvent> es = controller.getEventStore();
        Account a = es.materialize(new Account(), TEST_ACCOUNT);

        Assertions.assertEquals(expectedBalance, a.getBalance());
        Assertions.assertEquals(TEST_EVENT_COUNT+1, consumer.getEventCount());

        submgr.unsubscribe(OpenAccountEvent.class.getCanonicalName(), consumer);
        submgr.unsubscribe(BalanceChangeEvent.class.getCanonicalName(), consumer);
    }

    @Test
    /** Same as transactions test, except we do verification, compact 50%, then
     * verify again to ensure we get the same result.
     */
    void verifyCompaction() {
        AccountConsumer consumer = new AccountConsumer();
        submgr.subscribe(OpenAccountEvent.class.getCanonicalName(), consumer);
        submgr.subscribe(BalanceChangeEvent.class.getCanonicalName(), consumer);

        // Open the account
        OpenAccountEvent open = new OpenAccountEvent(TEST_ACCOUNT, "Test Account", BigDecimal.valueOf(100.00));
        System.out.println("Event 1 = " + open);
        controller.handleEvent(open);

        BigDecimal expectedBalance = BigDecimal.valueOf(100.00); // Set to initial balance

        // Throw transactions at the account
        System.out.println("Sending " + testEvents.size() + " randomized events");
        for (int i=0; i<testEvents.size(); i++) {
            AccountEvent event = testEvents.get(i);
            System.out.println("Event " + (i+2) + " = " + event);
            JSONObject jobj = new JSONObject(event.getPayload().getValue());
            expectedBalance = expectedBalance.add(jobj.getBigDecimal("balanceChange"));
            controller.handleEvent(testEvents.get(i));
        }

        // Get a materialized view that reflects the event
        EventStore<Account, String, AccountEvent> es = controller.getEventStore();
        Account a = es.materialize(new Account(), TEST_ACCOUNT);

        Assertions.assertEquals(expectedBalance, a.getBalance());
        Assertions.assertEquals(TEST_EVENT_COUNT+1, consumer.getEventCount());

        es.compact(new AccountCompactionEvent(), new Account(), "67890", 0.5);
        Account a2 = es.materialize(new Account(), TEST_ACCOUNT);

        Assertions.assertEquals(expectedBalance, a2.getBalance());
        // TODO: don't want messaging count here, but actual count ... haven't exposed
        //  getEventsFor but might need to for this text.
        //Assertions.assertEquals(51, consumer.getEventCount());

        submgr.unsubscribe(OpenAccountEvent.class.getCanonicalName(), consumer);
        submgr.unsubscribe(BalanceChangeEvent.class.getCanonicalName(), consumer);
    }
}
