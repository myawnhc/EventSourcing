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

import java.math.BigDecimal;

public class BalanceChangeEvent extends AccountEvent {

    public static final String QUAL_EVENT_NAME = "AccountService.BalanceChangeEvent";
    public static final String ACCT_NUM = "key";
    public static final String BALANCE_CHANGE = "balanceChange";

    BigDecimal balanceChange;

    public BalanceChangeEvent(String acctNumber, String eventName, BigDecimal change) {
        setEventName(QUAL_EVENT_NAME);
        this.key = acctNumber;
        this.balanceChange = change;
    }

    public BalanceChangeEvent(GenericRecord data) {
        setEventName(QUAL_EVENT_NAME);
        this.key = data.getString(ACCT_NUM);
        this.balanceChange = data.getDecimal(BALANCE_CHANGE);
    }

    public BalanceChangeEvent(SqlRow row) {
        setEventName(QUAL_EVENT_NAME);
        this.key = row.getObject("doKey");
        this.balanceChange = row.getObject(BALANCE_CHANGE);
    }

    public BigDecimal getBalanceChange() { return balanceChange; }

    @Override
    public GenericRecord apply(GenericRecord account) {
        BigDecimal newBalance = account.getDecimal(Account.FIELD_BALANCE).add(balanceChange);
        account = account.newBuilderWithClone().setDecimal(Account.FIELD_BALANCE, newBalance).build();
        return account;
    }

    @Override
    public String toString() {
        return eventName + " " + key + " " + balanceChange.toString();
    }

    @Override
    public GenericRecord toGenericRecord() {
        GenericRecord gr = GenericRecordBuilder.compact(getEventName())
                .setString(EVENT_NAME, QUAL_EVENT_NAME)
                .setString(ACCT_NUM, key)
                .setDecimal(BALANCE_CHANGE, balanceChange)
                .build();
        return gr;
    }
}
