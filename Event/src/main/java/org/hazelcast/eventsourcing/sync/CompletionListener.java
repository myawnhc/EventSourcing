/*
 * Copyright 2024 Hazelcast, Inc
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

import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import org.hazelcast.eventsourcing.event.PartitionedSequenceKey;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

/* Used by EventSourcingController.handleEvent.
   We construct a listener, which is then armed against the completions map.
   handleEvent then sends the event though the EventSourcingPipeline
   the final pipeline stage writes the event to the Completions Map, triggering
   the listener here to return the CompletionInfo (via a Future) to the original
   caller of handleEvent (the microservice).
 */
public class CompletionListener implements EntryUpdatedListener<PartitionedSequenceKey, GenericRecord> {
    final private UUID removalKey;
    private PartitionedSequenceKey psk;
    private IMap<PartitionedSequenceKey, GenericRecord> completionsMap;
    private static final Logger logger = Logger.getLogger(CompletionListener.class.getName());
    private CompletableFuture<CompletionInfo> future;

    public CompletionListener(IMap<PartitionedSequenceKey, GenericRecord> map,
                              PartitionedSequenceKey psk, CompletableFuture<CompletionInfo> future) {
        completionsMap = map;
        this.psk = psk;
        this.future = future;
        this.removalKey = map.addEntryListener(this, psk, true );
    }
    @Override
    public void entryUpdated(EntryEvent<PartitionedSequenceKey, GenericRecord> entryEvent) {
        PartitionedSequenceKey key = entryEvent.getKey();
        if (! key.equals(psk) ) logger.severe("Listener got unwanted key");
        // Reconstruct CompletionInfo from GenericRecord
        CompletionInfo info = new CompletionInfo(entryEvent.getValue());
        if (info.status != CompletionInfo.Status.COMPLETED_OK) {
            logger.warning("CompletionListener: Unexpected update: Updated but not complete!");
            return; // maybe there's another update to come?
        }
        future.complete(info);
        completionsMap.removeEntryListener(removalKey);
    }
}
