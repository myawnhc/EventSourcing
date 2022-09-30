package org.hazelcast.eventsourcing.testobjects;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.org.json.JSONObject;

import java.math.BigDecimal;

public class OpenAccountEventSerializer implements CompactSerializer<OpenAccountEvent> {
    @Override
    public OpenAccountEvent read(CompactReader compactReader) {
        String acctNumber = compactReader.readString("key");
        String eventClass = compactReader.readString("eventClass");
        long timestamp = compactReader.readInt64("timestamp");
        HazelcastJsonValue payload = compactReader.readCompact("payload");
        JSONObject jobj = new JSONObject(payload.getValue());
        String acctName = jobj.getString("accountName");
        BigDecimal initialBalance = jobj.getBigDecimal("initialBalance");
        OpenAccountEvent event = new OpenAccountEvent(acctNumber, acctName, initialBalance);
        event.setTimestamp(timestamp);
        event.setEventClass(eventClass);
        return event;
    }

    @Override
    public void write(CompactWriter compactWriter, OpenAccountEvent openAccountEvent) {
        compactWriter.writeString("key", openAccountEvent.getKey());
        compactWriter.writeString("eventClass", openAccountEvent.getEventClass());
        compactWriter.writeInt64("timestamp", openAccountEvent.getTimestamp());
        compactWriter.writeCompact("payload", openAccountEvent.getPayload());
    }

    @Override
    public String getTypeName() {
        return "org.hazelcast.eventsourcing.testobjects.OpenAccountEvent";
    }

    @Override
    public Class<OpenAccountEvent> getCompactClass() {
        return OpenAccountEvent.class;
    }
}
