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

package org.hazelcast.eventsourcing.testobjects;

import org.hazelcast.eventsourcing.pubsub.Consumer;

public class AccountConsumer implements Consumer<AccountEvent> {

    private int eventsReceived;
    @Override
    public void onEvent(AccountEvent eventMessage) {
        //System.out.println("Received event: " + eventMessage + " in consumer " + this);
        eventsReceived++;
    }

    public int getEventCount() {
        return eventsReceived;
    }
}
