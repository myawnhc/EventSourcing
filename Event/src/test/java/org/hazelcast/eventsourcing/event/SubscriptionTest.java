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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

public class SubscriptionTest {

    //static EventStore<Account, String, AccountEvent> store;
    static EventSourcingController<Account, String, AccountEvent> controller;
    static SubscriptionManager<AccountEvent> submgr;

    @BeforeAll
    static void init() {
        // Create event store
        HazelcastInstance embedded = Hazelcast.newHazelcastInstance();

        //store = new EventStore<>("accountMap", "accountID", Account::new, embedded);
        controller = EventSourcingController.newBuilder(embedded, "account")
                .domainObjectConstructor(Account::new)
                .build();

        // Create subscription manager, register it
        submgr = new ReliableTopicSubMgr<>();
        SubscriptionManager.register(embedded, OpenAccountEvent.class, submgr);
    }

    @AfterAll
    static void cleanUp() {}

    @BeforeEach
    void setUp() {}

    @AfterEach
    void tearDown() {}

    @Test
    void create() {
        // Account is a DomainObject
        //Account acct = new Account();

        AccountConsumer consumer = new AccountConsumer();
        submgr.subscribe(OpenAccountEvent.class.getCanonicalName(), consumer);

        // OpenAccountEvent is a SequencedEvent
        OpenAccountEvent open = new OpenAccountEvent("12345", "Bob", BigDecimal.valueOf(777.22));
        //store.append(open); // triggers publication - on the way out

        controller.handleEvent(open);

        // Verify: materialized account is as expected (correct balance)
        // Verify: event store is as expected (one event)
        // Verify: any state of subscription manager is as expected (one subscriber)
        // Verify: subscriber received event

        // Invoking getEventsFor directly until materialize is implemented.
        EventStore es = controller.getEventStore();
        es.getEventsFor("key", "12345");

        // Although we aren't explicitly calling shutdown we are nevertheless shutting down and
        // missing seeing logging output from our events ... add sleep until we can figure out
        // a better wait-for-results strategy.
        try {
            Thread.sleep(5000000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}
