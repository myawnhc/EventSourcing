package org.hazelcast.eventsourcing.testobjects;

import org.hazelcast.eventsourcing.pubsub.Consumer;

public class AccountConsumer implements Consumer<OpenAccountEvent> {
    @Override
    public void onEvent(OpenAccountEvent eventMessage) {
        System.out.println("Received event: " + eventMessage);
    }
}
