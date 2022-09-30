package org.hazelcast.eventsourcing.event;

import com.hazelcast.partition.PartitionAware;

import java.io.Serializable;

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

    @Override
    public K getPartitionKey() {
        return domainObjectKey;
    }

    @Override
    public int compareTo(Object o) {
        // Sequence numbers should be unique across all natural keys
        if (o instanceof PartitionedSequenceKey psk) {
            return Long.valueOf(sequence).compareTo(psk.sequence);
        }
        throw new IllegalArgumentException();
    }
}
