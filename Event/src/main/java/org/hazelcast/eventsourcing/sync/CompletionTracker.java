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

import org.hazelcast.eventsourcing.event.PartitionedSequenceKey;

import java.io.Serializable;
import java.util.concurrent.Future;

// Experimental.  Used as a Service within pipelines.
// EventSourcingController probably needs to create this and pass it into the
// ESPipeline; then make it available to external pipelines (which already have
// ESC reference to call handleEvent and either expose the CT object, or else
// just provide an API that passes data into the CT (maybe preferable so we
// maintain freedom to evolve this CT as needed.

// Could we perhaps overload/enhance handleEvent so that one call does it all?
//   handleEvent ( UUID, Event ) returns Future<CompletionInfo> ???


public class CompletionTracker implements Serializable {

    // OPEN QUESTIONS:
    // - organize around PSK or UUID?
    //   - in service, we use UUID throughout the entire pipeline
    //   - in ESF, we use PSK throughout the entire pipeline
    //   - We now expose PSK outside ESF as return from handleEvent - is this wise?
    // - return just CompletionInfo, or also Event?
    //   - we need data from event to build the request back to the caller

    /* If PSK is our key, call immediately after handleEvent.  If we make UUID
     * key then we can call just before handleEvent and eliminate the potential
     * race condition -
     */
    public Future register(PartitionedSequenceKey key) {
        return null;
    }

    /* Call from listener when info is updated */
    public void notify(PartitionedSequenceKey key, CompletionInfo info) {
        ;
    }
}
