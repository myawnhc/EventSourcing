package org.hazelcast.eventsourcing.testobjects;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.org.json.JSONObject;

import java.math.BigDecimal;

// see if we can use Compact and omit Serializable here ...
public class OpenAccountEvent extends AccountEvent {

    public OpenAccountEvent(String acctNumber, String acctName, BigDecimal initialBalance) {
        this.key = acctNumber;
        GenericRecord gr = GenericRecordBuilder.compact("org.hazelcast.eventsourcing.testobjects.OpenAccountEvent.payload")
                .setString("accountName", acctName)
                .setDecimal("initialBalance", initialBalance)
                .build();
        setPayload(gr);
    }


    public HazelcastJsonValue getPayloadAsJSON() {
        JSONObject jobj = new JSONObject();
        jobj.put("accountName", getPayload().getString("accountName"));
        jobj.put("initialBalance", getPayload().getDecimal("initialBalance"));
        HazelcastJsonValue hjv = new HazelcastJsonValue(jobj.toString());
        System.out.println("hjv = " + hjv);
        return hjv;
    }

    @Override
    public Account apply(Account account) {
        // For an open accunt transaction, there is no pre-existing account ...
        // we expect to be passed null and will build from scratch.
        account = new Account();
        account.setAccountNumber(key);
        account.setAccountName(payload.getString("accountName"));
        account.setBalance(payload.getDecimal("initialBalance"));
        return account;
    }

    @Override
    public String toString() {
        return "OpenAccountEvent " + key;
    }
}
