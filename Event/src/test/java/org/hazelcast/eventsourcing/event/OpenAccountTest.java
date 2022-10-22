package org.hazelcast.eventsourcing.event;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.hazelcast.eventsourcing.EventSourcingController;
import org.hazelcast.eventsourcing.eventstore.EventStore;
import org.hazelcast.eventsourcing.pubsub.SubscriptionManager;
import org.hazelcast.eventsourcing.pubsub.impl.ReliableTopicSubMgr;
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

public class OpenAccountTest {

    static EventSourcingController<Account, String, AccountEvent> controller;
    static SubscriptionManager<AccountEvent> submgr;
    static HazelcastInstance hazelcast;

    @BeforeAll
    static void init() {
        // Create Hazelcast structures
        hazelcast = Hazelcast.newHazelcastInstance();
        controller = EventSourcingController.<Account,String,AccountEvent>newBuilder(hazelcast, "account")
                .build();

        // Create subscription manager, register it
        submgr = new ReliableTopicSubMgr<>();
        SubscriptionManager.register(hazelcast, OpenAccountEvent.class, submgr);
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
    void verifyInitialBalance() {
        AccountConsumer consumer = new AccountConsumer();
        submgr.subscribe(OpenAccountEvent.class.getCanonicalName(), consumer);

        // OpenAccountEvent is a SourcedEvent
        OpenAccountEvent open = new OpenAccountEvent("12345", "Bob", BigDecimal.valueOf(777.22));
        controller.handleEvent(open);

        // Get a materialized view that reflects the event
        EventStore<Account, String, AccountEvent> es = controller.getEventStore();
        Account a = es.materialize(new Account(), "12345");

        Assertions.assertEquals(BigDecimal.valueOf(777.22), a.getBalance());
        Assertions.assertEquals(1, consumer.getEventCount());
    }
}
