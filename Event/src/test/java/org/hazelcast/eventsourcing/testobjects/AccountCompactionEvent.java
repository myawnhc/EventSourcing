package org.hazelcast.eventsourcing.testobjects;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.org.json.JSONObject;
import com.hazelcast.sql.SqlRow;
import org.hazelcast.eventsourcing.eventstore.EventStoreCompactionEvent;

import java.io.Serializable;
import java.math.BigDecimal;

public class AccountCompactionEvent extends AccountEvent
        implements EventStoreCompactionEvent<Account>, Serializable {

    private String accountNumber;
    private String accountName;
    private BigDecimal balance;

    // Used to pass empty item into materialize that will get fields initialized from the
    // materialization process.  Considered experimental for now ...
    public AccountCompactionEvent() {
        this.eventClass = AccountCompactionEvent.class.getCanonicalName();
        JSONObject jobj = new JSONObject();
        setPayload(new HazelcastJsonValue(jobj.toString()));
    }

    public AccountCompactionEvent(String acctNumber, String acctName, BigDecimal balance) {
        this.key = acctNumber;
        this.eventClass = AccountCompactionEvent.class.getCanonicalName();
        JSONObject jobj = new JSONObject();
        jobj.put("accountName", acctName);
        jobj.put("balance", balance);
        setPayload(new HazelcastJsonValue(jobj.toString()));
    }

    public AccountCompactionEvent(SqlRow row) {
        this.accountNumber = row.getObject("key");
        this.key = accountNumber;
        HazelcastJsonValue payload = row.getObject("payload");
        JSONObject jobj = new JSONObject(payload.getValue());
        this.accountName = jobj.getString("accountName");
        this.balance = jobj.getBigDecimal("balance");
        setPayload(payload);
        eventClass = AccountCompactionEvent.class.getCanonicalName();
        setTimestamp(row.getObject("timestamp"));
    }

    @Override
    public Account apply(Account account) {
        account.setAccountNumber(accountNumber);
        account.setAccountName(accountName);
        account.setBalance(balance);
        return account;
    }

    @Override
    public void initFromDomainObject(Account domainObject) {
        this.accountNumber = domainObject.getAccountNumber();
        this.key = accountNumber; // Required and easy to miss - maybe require on constructor instead?
        this.accountName = domainObject.getAccountName();
        this.balance = domainObject.getBalance();
        JSONObject jobj = new JSONObject();
        jobj.put("accountName", accountName);
        jobj.put("balance", balance);
        setPayload(new HazelcastJsonValue(jobj.toString()));
    }

    @Override
    public String toString() {
        return "AccountCompactionEvent " + key;
    }
}
