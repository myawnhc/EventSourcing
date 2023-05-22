/*
 * Copyright 2022-2023 Hazelcast, Inc
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

package org.hazelcast.eventsourcing.sync;

import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import org.hazelcast.eventsourcing.event.SourcedEvent;

import java.util.UUID;

public class CompletionInfoCompactSerializer implements CompactSerializer<CompletionInfo> {
    @Override
    public CompletionInfo read(CompactReader compactReader) {
        UUID uuid = null;
        // UUID is optional, we'll get 'invalid field name' errors on deserialization if
        // we don't specifically verify it is present
        if (compactReader.getFieldKind("uuid") == FieldKind.STRING) {
            String uuidAsString = compactReader.readString("uuid");
            if (uuidAsString != null) {
                uuid = UUID.fromString(uuidAsString);
            }
        }
        SourcedEvent event = compactReader.readCompact("event");
        CompletionInfo info = new CompletionInfo(event, uuid);
        // Careful here - serializing enum as compact results in enums that don't match!
        info.status = CompletionInfo.Status.valueOf(compactReader.readString("status"));
        info.startTime = compactReader.readInt64("startTime");
        info.completionTime = compactReader.readInt64("completionTime");
        info.error = compactReader.readCompact("error");
        return info;
    }

    @Override
    public void write(CompactWriter compactWriter, CompletionInfo completionInfo) {
        compactWriter.writeString("status", completionInfo.status.name());
        compactWriter.writeInt64("startTime", completionInfo.startTime);
        compactWriter.writeInt64("completionTime", completionInfo.completionTime);
        compactWriter.writeCompact("error", completionInfo.error);
        if (completionInfo.getUUID() != null)
            compactWriter.writeString("uuid", completionInfo.getUUID().toString());
        else
            compactWriter.writeString("uuid", null); // otherwise
        compactWriter.writeCompact("event", completionInfo.getEvent());
    }

    @Override
    public String getTypeName() {
        return CompletionInfo.class.getCanonicalName();
    }

    @Override
    public Class<CompletionInfo> getCompactClass() {
        return CompletionInfo.class;
    }
}
