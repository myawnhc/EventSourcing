package org.hazelcast.eventsourcing.testobjects;

import org.hazelcast.eventsourcing.event.SourcedEvent;

import java.util.function.UnaryOperator;

public abstract class AccountEvent extends SourcedEvent<Account, String> implements UnaryOperator<Account> {
}
