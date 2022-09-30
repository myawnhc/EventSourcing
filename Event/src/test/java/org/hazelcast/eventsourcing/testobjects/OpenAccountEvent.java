package org.hazelcast.eventsourcing.testobjects;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.org.json.JSONObject;
import com.hazelcast.sql.SqlRow;

import java.math.BigDecimal;

public class OpenAccountEvent extends AccountEvent {

    public OpenAccountEvent(String acctNumber, String acctName, BigDecimal initialBalance) {
        this.key = acctNumber;
        this.eventClass = OpenAccountEvent.class.getCanonicalName();
        JSONObject jobj = new JSONObject();
        jobj.put("accountName", acctName);
        jobj.put("initialBalance", initialBalance);
        setPayload(new HazelcastJsonValue(jobj.toString()));
    }

    // Reconstruct an event from its SQL stored format
    public OpenAccountEvent(SqlRow row) {
        this.key = row.getObject("key");
        HazelcastJsonValue payload = row.getObject("payload");
        setPayload(payload);
        eventClass = OpenAccountEvent.class.getCanonicalName();
        setTimestamp(row.getObject("timestamp"));
    }

    @Override
    public Account apply(Account account) {
        // When called from pipeline we will be passed null as there is no
        // entry for the account found when doing initial lookup
        if (account == null)
            account = new Account();
        JSONObject jobj = new JSONObject(payload.getValue());
        account.setAccountNumber(key);
        account.setAccountName(jobj.getString("accountName"));
        account.setBalance(jobj.getBigDecimal("initialBalance"));
        return account;
    }

    @Override
    public String toString() {
        return "OpenAccountEvent " + key;
    }
}
