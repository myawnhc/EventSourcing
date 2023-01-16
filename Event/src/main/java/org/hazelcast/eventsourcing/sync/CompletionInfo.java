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

import java.io.Serializable;

public class CompletionInfo implements Serializable {

    public enum Status { INCOMPLETE, COMPLETED_OK, TIMED_OUT, HAD_ERROR }

    public Status status;
    public long startTime;
    public long completionTime = -1;
    public Throwable error;

    public CompletionInfo() {
        status = Status.INCOMPLETE;
        startTime = System.currentTimeMillis();
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
}
