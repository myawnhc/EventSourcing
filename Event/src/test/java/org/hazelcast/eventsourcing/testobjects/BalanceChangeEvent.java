package org.hazelcast.eventsourcing.testobjects;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.org.json.JSONObject;
import com.hazelcast.sql.SqlRow;

import java.math.BigDecimal;

public class BalanceChangeEvent extends AccountEvent {

    public BalanceChangeEvent(String acctNumber, String eventName, BigDecimal change) {
        this.key = acctNumber;
        this.eventClass = BalanceChangeEvent.class.getCanonicalName();
        JSONObject jobj = new JSONObject();
        jobj.put("balanceChange", change);
        jobj.put("eventName", eventName);
        setPayload(new HazelcastJsonValue(jobj.toString()));
    }

    public BalanceChangeEvent(SqlRow row) {
        this.key = row.getObject("key");
        HazelcastJsonValue payload = row.getObject("payload");
        setPayload(payload);
        eventClass = BalanceChangeEvent.class.getCanonicalName();
        setTimestamp(row.getObject("timestamp"));
    }

    @Override
    public Account apply(Account account) {
        JSONObject jobj = new JSONObject(payload.getValue());
        account.setBalance(account.getBalance().add(jobj.getBigDecimal("balanceChange")));
        return account;
    }

    @Override
    public String toString() {
        JSONObject jobj = new JSONObject(getPayload().getValue());
        String eventName = jobj.getString("eventName");
        BigDecimal amount = jobj.getBigDecimal("balanceChange");
        return eventName + " " + key + " " + amount.toString();
    }
}
