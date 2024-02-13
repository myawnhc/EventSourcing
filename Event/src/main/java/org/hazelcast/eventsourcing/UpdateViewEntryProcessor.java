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
 */

package org.hazelcast.eventsourcing;

import com.hazelcast.core.Offloadable;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import org.hazelcast.eventsourcing.event.DomainObject;
import org.hazelcast.eventsourcing.event.SourcedEvent;

import java.io.Serializable;
import java.util.Map;
import java.util.logging.Logger;

public class UpdateViewEntryProcessor<D extends DomainObject<K>, K extends Comparable<K>, E extends SourcedEvent<D, K>>
        implements EntryProcessor<K, GenericRecord, GenericRecord>, Offloadable, Serializable {

    private SourcedEvent<D, K> event;
    private static final Logger logger = Logger.getLogger(UpdateViewEntryProcessor.class.getName());


    public UpdateViewEntryProcessor(SourcedEvent<D, K> event) {
        this.event = event;
    }

    @Override
    public String getExecutorName() {
        return Offloadable.OFFLOADABLE_EXECUTOR;
    }

    @Override
    public synchronized GenericRecord process(Map.Entry<K, GenericRecord> viewEntry) {
        GenericRecord domainObject = viewEntry.getValue();
        GenericRecord updatedDO = event.apply(domainObject);
        viewEntry.setValue(updatedDO);
        return updatedDO;
    }
}
