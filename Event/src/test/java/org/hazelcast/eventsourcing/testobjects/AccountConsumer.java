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

package org.hazelcast.eventsourcing.testobjects;

import org.hazelcast.eventsourcing.pubsub.Consumer;

public class AccountConsumer implements Consumer<AccountEvent> {

    private int eventsReceived;

    // While debugging missing items, breaking them down ...
    private int openEvents = 0;
    private int balanceChangeEvents = 0;
    private int compactionEvents = 0;
    private int markerEvents = 0;

    @Override
    public synchronized void onEvent(AccountEvent eventMessage) {
        // Can miscount events if unsynchronized
        if (eventMessage.getEventName().contains("OpenAccountEvent"))
            openEvents++;
        else if (eventMessage.getEventName().contains("BalanceChangeEvent"))
            balanceChangeEvents++;
        else if (eventMessage.getEventName().contains("AccountCompactionEvent"))
            compactionEvents++;
        else if (eventMessage.getEventName().contains("AccountMarkerEvent"))
            markerEvents++;
        //System.out.println("Received event: " + eventMessage + " in consumer " + this);
        eventsReceived++;
    }

    public int getEventCount() {
        System.out.println("--- " + openEvents + " open events");
        System.out.println("--- " + balanceChangeEvents + " balance change events");
        if (compactionEvents > 0)
            System.out.println("--- " + compactionEvents + " compaction events");
        if (markerEvents > 0)
            System.out.println("--- " + markerEvents + " marker events");

        return eventsReceived;
    }
}
