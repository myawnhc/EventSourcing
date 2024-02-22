/*
 * Copyright 2023 Hazelcast, Inc
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

import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import org.hazelcast.eventsourcing.event.SourcedEvent;

import java.io.Serializable;
import java.util.UUID;

public class CompletionInfo implements Serializable {

    public enum Status { INCOMPLETE, PIPELINE_ENTRY, PIPELINE_EXIT, COMPLETED_OK, TIMED_OUT, HAD_ERROR }

    public Status status;
    public long startTime;
    public long completionTime = -1;
    public Throwable error;

    private UUID uuid;
    private GenericRecord eventAsGR;
    private String eventName;
    private Object eventKey;

    public CompletionInfo(GenericRecord event, UUID identifier) {
        status = Status.INCOMPLETE;
        startTime = System.currentTimeMillis();
        this.uuid = identifier;
        this.eventAsGR = event;
        this.eventName = event.getString(SourcedEvent.EVENT_NAME);
        switch(eventAsGR.getFieldKind("key")) {
            case STRING:
                this.eventKey = eventAsGR.getString("key");
                break;
            case COMPACT:
                this.eventKey = eventAsGR.getGenericRecord("key");
                break;
            default:
                System.out.println("Unexpected field type " + eventAsGR.getFieldKind("key").name());
        }    }

    public CompletionInfo(GenericRecord data) {
        this.status = Status.valueOf(data.getString("status"));
        this.uuid = UUID.fromString(data.getString("uuid"));
        this.startTime = data.getInt64("startTime");
        this.completionTime = data.getInt64("completionTime");
        this.eventAsGR = data.getGenericRecord("event");
        this.eventName = eventAsGR.getString("eventName");
        switch(eventAsGR.getFieldKind("key")) {
            case STRING:
                this.eventKey = eventAsGR.getString("key");
                break;
            case COMPACT:
                this.eventKey = eventAsGR.getGenericRecord("key");
                break;
            default:
                System.out.println("Unexpected field type " + eventAsGR.getFieldKind("key").name());
        }
    }

    public void markComplete() {
        status = Status.COMPLETED_OK;
        completionTime = System.currentTimeMillis();
    }

    public void pipelineEntry() {
        status = Status.PIPELINE_ENTRY;
    }

    public void pipelineExit() {
        status = Status.PIPELINE_EXIT;
    }

    public void markFailed(Throwable t) {
        status = Status.HAD_ERROR;
        error = t;
        completionTime = System.currentTimeMillis();
    }

    public void markOutOfTime() {
        status = Status.TIMED_OUT;
        completionTime = System.currentTimeMillis();
    }

    public UUID getUUID() {
        return uuid;
    }

    public GenericRecord getEvent() {
        return eventAsGR;
    }
    public String getEventName() { return eventName; }

    public Object getEventKey() { return eventKey; }

    @Override
    public String toString() {
        return "CompletionInfo " +
                eventName + " " +
                eventKey + " " +
                status.name();
    }

    public GenericRecord toGenericRecord() {
        GenericRecord gr = GenericRecordBuilder.compact("CompletionInfo")
                .setString("status", status.toString())
                .setString("uuid", uuid.toString())
                .setInt64("startTime", startTime)
                .setInt64("completionTime", completionTime)
                .setGenericRecord("event", eventAsGR)
                // Error, if any, will not be serialized.  Maybe capture just the message?
                .build();
        return gr;
    }
}
