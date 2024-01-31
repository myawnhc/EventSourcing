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

import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import org.hazelcast.eventsourcing.event.PartitionedSequenceKey;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public class CompletionTracker implements EntryUpdatedListener<PartitionedSequenceKey, GenericRecord> {

    private Map<PartitionedSequenceKey, CompletableFuture<CompletionInfo>> futures = new HashMap<>(); // registrations
    private static final Logger logger = Logger.getLogger(CompletionTracker.class.getName());

    public synchronized CompletableFuture<CompletionInfo> register(PartitionedSequenceKey key) {
        CompletableFuture<CompletionInfo> future = new CompletableFuture<>();
        futures.put(key, future);
        return future;
    }

    @Override
    public synchronized void entryUpdated(EntryEvent<PartitionedSequenceKey, GenericRecord> entryEvent) {
        //System.out.println("CompletionTracker entryUpdated listener triggered");
        try {
            PartitionedSequenceKey key = entryEvent.getKey();
            // Reconstruct CompletionInfo from GenericRecord
            CompletionInfo info = new CompletionInfo(entryEvent.getValue());
            if (info.status != CompletionInfo.Status.COMPLETED_OK) {
                logger.info("Unexpected update: Updated but not complete!");
                return; // maybe there's another update to come?
            }
            CompletableFuture<CompletionInfo> future = futures.get(key);
            if (future == null) {
                System.out.println("No future found for " + key);
            } else {
                //logger.info("CompletionTracker entryUpdated, marking future complete " + info);
                future.complete(info);
                futures.remove(key);
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
