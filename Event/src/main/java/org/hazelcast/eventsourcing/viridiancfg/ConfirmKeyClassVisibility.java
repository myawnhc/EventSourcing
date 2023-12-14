/*
 * Copyright 2022 Hazelcast, Inc
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

package org.hazelcast.eventsourcing.viridiancfg;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import org.hazelcast.eventsourcing.event.PartitionedSequenceKey;

import java.io.Serializable;
import java.util.logging.Logger;

/** Debugging why PartitionedSequenceKey is not visible on server side even though
 *  it is on the classpath of the member
 */
public class ConfirmKeyClassVisibility implements Runnable, Serializable, HazelcastInstanceAware {
    private transient HazelcastInstance hazelcast;
    private static final Logger logger = Logger.getLogger(ConfirmKeyClassVisibility.class.getName());

    public ConfirmKeyClassVisibility() {

    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcast = hazelcastInstance;
    }

    @Override
    public void run() {
        logger.info("In server-side runnable, PSK = " + PartitionedSequenceKey.class);
    }
}