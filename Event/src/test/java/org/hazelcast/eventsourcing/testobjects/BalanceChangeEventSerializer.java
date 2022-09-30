package org.hazelcast.eventsourcing.testobjects;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.org.json.JSONObject;

import java.math.BigDecimal;

public class BalanceChangeEventSerializer implements CompactSerializer<BalanceChangeEvent> {
    @Override
    public BalanceChangeEvent read(CompactReader compactReader) {
        String acctNumber = compactReader.readString("key");
        String eventClass = compactReader.readString("eventClass");
        long timestamp = compactReader.readInt64("timestamp");
        HazelcastJsonValue payload = compactReader.readCompact("payload");
        JSONObject jobj = new JSONObject(payload.getValue());
        BigDecimal balanceChange = jobj.getBigDecimal("balanceChange");
        String eventName = jobj.getString("eventName");
        BalanceChangeEvent event = new BalanceChangeEvent(acctNumber, eventName, balanceChange);
        event.setTimestamp(timestamp);
        event.setEventClass(eventClass);
        return event;
    }

    @Override
    public void write(CompactWriter compactWriter, BalanceChangeEvent balanceChangeEvent) {
        compactWriter.writeString("key", balanceChangeEvent.getKey());
        compactWriter.writeString("eventClass", balanceChangeEvent.getEventClass());
        compactWriter.writeInt64("timestamp", balanceChangeEvent.getTimestamp());
        compactWriter.writeCompact("payload", balanceChangeEvent.getPayload());
    }

    @Override
    public String getTypeName() {
        return "org.hazelcast.eventsourcing.testobjects.BalanceChangeEvent";
    }

    @Override
    public Class<BalanceChangeEvent> getCompactClass() {
        return BalanceChangeEvent.class;
    }
}
