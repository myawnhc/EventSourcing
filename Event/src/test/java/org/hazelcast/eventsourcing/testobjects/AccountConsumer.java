package org.hazelcast.eventsourcing.testobjects;

import org.hazelcast.eventsourcing.pubsub.Consumer;

public class AccountConsumer implements Consumer<AccountEvent> {

    private int eventsReceived;
    @Override
    public void onEvent(AccountEvent eventMessage) {
        System.out.println("Received event: " + eventMessage + " in consumer " + this);
        eventsReceived++;
    }

    public int getEventCount() {
        System.out.println("Querying event count in consumer " + this);
        return eventsReceived;
    }
}
