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
        long timestamp = compactReader.readInt64("timestamp");
//        String acctName = compactReader.readString("accountName");
//        BigDecimal initialBalance = compactReader.readDecimal("initialBalance");
        //GenericRecord payload = compactReader.readCompact("payload");
        HazelcastJsonValue payload = compactReader.readCompact("payload");
        JSONObject jobj = new JSONObject(payload.getValue());
        String acctName = jobj.getString("accountName");
        BigDecimal initialBalance = jobj.getBigDecimal("initialBalance");
//        String acctName = payload.getString("accountName");
//        BigDecimal initialBalance = payload.getDecimal("initialBalance");
        OpenAccountEvent event = new OpenAccountEvent(acctNumber, acctName, initialBalance);
        event.setTimestamp(timestamp);
        return event;
    }

    @Override
    public void write(CompactWriter compactWriter, OpenAccountEvent openAccountEvent) {
        compactWriter.writeString("key", openAccountEvent.getKey());
        compactWriter.writeInt64("timestamp", openAccountEvent.getTimestamp());
        // GenericRecord is not serializable directly, need to disassemble the pieces
        compactWriter.writeCompact("payload", openAccountEvent.getPayloadAsJSON());
//        compactWriter.writeString("accountName", openAccountEvent.getPayload().getString("accountName"));
//        compactWriter.writeDecimal("initialBalance", openAccountEvent.getPayload().getDecimal("initialBalance"));
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
