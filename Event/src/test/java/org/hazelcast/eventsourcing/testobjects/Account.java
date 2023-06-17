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

import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.sql.SqlRow;
import org.hazelcast.eventsourcing.event.DomainObject;

import java.math.BigDecimal;

public class Account implements DomainObject<String> {
    private String accountNumber;
    private String accountName;
    private BigDecimal balance;

    public Account() {}

    public Account(GenericRecord fromGR) {
        this.accountNumber = fromGR.getString("key");
        this.accountName = fromGR.getString("accountName");
        this.balance = fromGR.getDecimal("balance");
    }

    public Account(SqlRow data) {
        throw new RuntimeException("Account hydration from SqlRow unimplemented");
    }

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

    public GenericRecord toGenericRecord() {
        GenericRecord gr = GenericRecordBuilder.compact("AccountService.account")
                .setString("key", accountNumber)
                .setString("accountName", accountName)
                .setDecimal("balance", balance)
                .build();
        return gr;
    }
}
