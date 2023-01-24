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

import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.listener.EntryUpdatedListener;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

/** Adapts an EntryUpdatedListener&lt;T&gt; to a CompletableFuture&lt;T&gt; */
public class CompletionMapListenerFuture<K,V> extends CompletableFuture<V>
                                           implements EntryUpdatedListener<K,V>,
                                                      Serializable {

    @Override
    public void entryUpdated(EntryEvent<K, V> entryEvent) {
        super.complete(entryEvent.getValue());
    }
}
