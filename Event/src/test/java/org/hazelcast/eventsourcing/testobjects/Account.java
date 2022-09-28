package org.hazelcast.eventsourcing.testobjects;

import org.hazelcast.eventsourcing.event.DomainObject;

import java.math.BigDecimal;

public class Account implements DomainObject<String> {
    private String accountNumber;
    private String accountName;
    private BigDecimal balance;

    public String getAccountNumber() {
        return accountNumber;
    }

    public void setAccountNumber(String accountNumber) {
        this.accountNumber = accountNumber;
    }

    public String getAccountName() {
        return accountName;
    }

    public void setAccountName(String accountName) {
        this.accountName = accountName;
    }

    public BigDecimal getBalance() {
        return balance;
    }

    public void setBalance(BigDecimal balance) {
        this.balance = balance;
    }

    @Override
    public String getKey() {
        return accountNumber;
    }
}
