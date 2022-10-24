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

package org.hazelcast.eventsourcing.event;

import com.hazelcast.partition.PartitionAware;

import java.io.Serializable;

/** This serves as the key type for both the Pending Events map and the Event Store.
 * The key consists of two components, a domain object key (type varies, but will be cast
 * as a VARCHAR in SQL queries) and a sequence number.
 *
 * Sequence numbers are generally unique across an event store (more specifically, they are
 * generated at the store level and not at the domain object key level).  When compacting
 * the store, compaction records are given a sequence of zero, so there can be multiple
 * records with sequence zero but different domain keys.
 *
 * @param <K> the domain object key type
 */
public class PartitionedSequenceKey<K> implements PartitionAware<K>, Comparable<K>, Serializable {

    K domainObjectKey;
    long sequence;

    public PartitionedSequenceKey(long sequence, K domainObjectKey) {
        this.domainObjectKey = domainObjectKey;
        this.sequence = sequence;
    }

    public long getSequence() {
        return sequence;
    }

    /** By implementing PartitionAware, keys for a domain object will be kept together on the
     * same cluster node. This improves performance of the materialize methods of the
     * EventStore as well as many queries; in addition it ensures that the MapJournal of
     * the Pending Events Map will be seen in order (as ordering of the map journal is not
     * maintained across partitions).
     * @return the partition key, which happens to be the domain object
     */
    @Override
    public K getPartitionKey() {
        return domainObjectKey;
    }

    @Override
    public int compareTo(Object o) {
        // Sequence numbers should be unique across all natural keys
        if (o instanceof PartitionedSequenceKey) {
            return Long.compare(sequence, ((PartitionedSequenceKey<K>) o).sequence);
        }
        throw new IllegalArgumentException();
    }
}
