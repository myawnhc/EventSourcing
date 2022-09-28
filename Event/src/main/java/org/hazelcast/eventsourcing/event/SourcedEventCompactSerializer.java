package org.hazelcast.eventsourcing.event;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;

@Deprecated // needs to be at lowest level where we know all the fields of each subtype
public class SourcedEventCompactSerializer<D extends DomainObject,K> implements CompactSerializer<SourcedEvent> {
    @Override
    public SourcedEvent read(CompactReader compactReader) {
        K key = compactReader.readCompact("key");
        long timestamp = compactReader.readInt64("timestamp");
        GenericRecord payload = compactReader.readCompact("payload");
        return null;  // TODO
    }

    @Override
    public void write(CompactWriter compactWriter, SourcedEvent sourcedEvent) {
        compactWriter.writeCompact("key", sourcedEvent.getKey());
        compactWriter.writeInt64("timestamp", sourcedEvent.getTimestamp());
        compactWriter.writeCompact("payload", sourcedEvent.getPayload());
    }

    @Override
    public String getTypeName() {
        return "org.hazelcast.eventsourcing.event.SourcedEvent";
    }

    @Override
    public Class<SourcedEvent> getCompactClass() {
        return SourcedEvent.class;
    }
}
