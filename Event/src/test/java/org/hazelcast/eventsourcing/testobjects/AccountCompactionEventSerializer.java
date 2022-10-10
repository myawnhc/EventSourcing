package org.hazelcast.eventsourcing.testobjects;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.org.json.JSONObject;

import java.math.BigDecimal;

public class AccountCompactionEventSerializer implements CompactSerializer<AccountCompactionEvent> {
    @Override
    public AccountCompactionEvent read(CompactReader compactReader) {
        String acctNumber = compactReader.readString("key");
        String eventClass = compactReader.readString("eventClass");
        long timestamp = compactReader.readInt64("timestamp");
        HazelcastJsonValue payload = compactReader.readCompact("payload");
        JSONObject jobj = new JSONObject(payload.getValue());
        BigDecimal balance = jobj.getBigDecimal("balance");
        String acctName = jobj.getString("acctName");
        AccountCompactionEvent event = new AccountCompactionEvent(acctNumber, acctName, balance);

        event.setTimestamp(timestamp);
        event.setEventClass(eventClass);
        return event;
    }

    @Override
    public void write(CompactWriter compactWriter, AccountCompactionEvent compactionEvent) {
        compactWriter.writeString("key", compactionEvent.getKey());
        compactWriter.writeString("eventClass", compactionEvent.getEventClass());
        compactWriter.writeInt64("timestamp", compactionEvent.getTimestamp());
        compactWriter.writeCompact("payload", compactionEvent.getPayload());
    }

    @Override
    public String getTypeName() {
        return "org.hazelcast.eventsourcing.testobjects.AccountCompactionEvent";
    }

    @Override
    public Class<AccountCompactionEvent> getCompactClass() {
        return AccountCompactionEvent.class;
    }
}
