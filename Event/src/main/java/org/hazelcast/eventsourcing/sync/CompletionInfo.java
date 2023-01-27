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

import org.hazelcast.eventsourcing.event.SourcedEvent;

import java.util.UUID;

public class CompletionInfo {

    public enum Status { INCOMPLETE, COMPLETED_OK, TIMED_OUT, HAD_ERROR }

    public Status status;
    public long startTime;
    public long completionTime = -1;
    public Throwable error; // not compact serializable - may change to just message string

    // By carrying these fields passed in by the pipeline we can simplify the flow
    private UUID uuid;
    public SourcedEvent event;

    // identifier may be null; used in pipeline callers but not unit tests or API-direct calls
    public CompletionInfo(SourcedEvent event, UUID identifier) {
        status = Status.INCOMPLETE;
        startTime = System.currentTimeMillis();
        this.uuid = identifier;
        this.event = event;
    }

    public void markComplete() {
        status = Status.COMPLETED_OK;
        completionTime = System.currentTimeMillis();
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

    public SourcedEvent getEvent() {
        return event;
    }
}
