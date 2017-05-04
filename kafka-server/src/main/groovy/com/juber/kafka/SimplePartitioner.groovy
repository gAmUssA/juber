package com.juber.kafka

import groovy.transform.CompileStatic
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

@CompileStatic
class SimplePartitioner implements Partitioner {

    @Override
    int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int partition = 0
        String stringKey = (String) key
        int offset = stringKey.lastIndexOf('.')
        if (offset > 0) {
            partition = Integer.parseInt(stringKey.substring(offset + 1)) % cluster.partitionCountForTopic(topic)
        }
        return partition
    }

    @Override
    void close() {}

    @Override
    void configure(Map<String, ?> configs) {}
}
