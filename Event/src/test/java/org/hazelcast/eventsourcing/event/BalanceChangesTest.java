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

import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import org.hazelcast.eventsourcing.eventstore.EventStore;
import org.hazelcast.eventsourcing.sync.CompletionInfo;
import org.hazelcast.eventsourcing.testobjects.Account;
import org.hazelcast.eventsourcing.testobjects.AccountConsumer;
import org.hazelcast.eventsourcing.testobjects.AccountEvent;
import org.hazelcast.eventsourcing.testobjects.AccountMarkerEvent;
import org.hazelcast.eventsourcing.testobjects.BalanceChangeEvent;
import org.hazelcast.eventsourcing.testobjects.OpenAccountEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class BalanceChangesTest extends TransactionsTestBase {

    @Test
    void verifyTransactions() {
        AccountConsumer consumer = new AccountConsumer();
        submgr.subscribe(OpenAccountEvent.QUAL_EVENT_NAME, consumer);
        submgr.subscribe(BalanceChangeEvent.QUAL_EVENT_NAME, consumer);
        submgr.subscribe(AccountMarkerEvent.QUAL_EVENT_NAME, consumer);

        // Open the account
        OpenAccountEvent open = new OpenAccountEvent(TEST_ACCOUNT, "Test Account", BigDecimal.valueOf(100.00));
        //System.out.println("Event 1 = " + open);
        CompletableFuture<CompletionInfo> openFuture = controller.handleEvent(open, UUID.randomUUID());
        System.out.println("Waiting up to 5 seconds for open account success ...");
        try {
            // Com
            CompletionInfo openInfo = openFuture.toCompletableFuture().get(5, TimeUnit.SECONDS);
            //CompletionInfo openInfo = openFuture.getA(5, TimeUnit.SECONDS);

            if (openInfo.status != CompletionInfo.Status.COMPLETED_OK) {
                Assertions.fail("Open account failed " + openInfo.status);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

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

        System.out.println("Writing EOD marker");
        AccountMarkerEvent eod = new AccountMarkerEvent(TEST_ACCOUNT);
        eod.addListener(new MarkerEventListener());
        CompletableFuture<CompletionInfo> eodCompleted = controller.handleEvent(eod, UUID.randomUUID());
        CompletionInfo eodInfo = null;
        try {
            System.out.println("Waiting up to 100 seconds for pipeline to clear ...");
            eodInfo = eodCompleted.get(100, TimeUnit.SECONDS);
            // We can still have a delay in getting notifications from the publisher,
            // adding another second seems to improve our success rate
            Thread.sleep(1000);
        } catch (Exception e) {
            Assertions.fail(e);
        }
        if (eodInfo.status == CompletionInfo.Status.COMPLETED_OK) {
            EventStore<Account, String, AccountEvent> es = controller.getEventStore();
            GenericRecord a = es.materialize(new Account().toGenericRecord(), TEST_ACCOUNT);

            Assertions.assertEquals(expectedBalance, a.getDecimal(Account.FIELD_BALANCE));
            // +1 for Open event, +1 for EOD Marker event
            Assertions.assertEquals(TEST_EVENT_COUNT+2, consumer.getEventCount());
            System.out.println("Test complete - All assertions passed");
        } else {
           Assertions.fail("Completion status for EOD = " + eodInfo.status);
        }

        submgr.unsubscribe(OpenAccountEvent.QUAL_EVENT_NAME, consumer);
        submgr.unsubscribe(BalanceChangeEvent.QUAL_EVENT_NAME, consumer);
        submgr.unsubscribe(AccountMarkerEvent.QUAL_EVENT_NAME, consumer);
    }
}
