package com.juber.hasselhoff.jet

import com.hazelcast.jet.DAG
import com.hazelcast.jet.Processors
import com.hazelcast.jet.connector.kafka.StreamKafkaP
import com.juber.kafka.MessageSerializer
import groovy.transform.CompileStatic
import org.apache.kafka.common.serialization.StringDeserializer

import static com.hazelcast.jet.Edge.between
import static com.juber.kafka.KafkaServerStarter.props

@CompileStatic
class Kafka2MapDAG {
    DAG dag() {
        def dag = new DAG()
        Properties props = props(
                "group.id", "group-" + Math.random(),
                "bootstrap.servers", "localhost:9092",
                "key.deserializer", StringDeserializer.class.canonicalName,
                "value.deserializer", MessageSerializer.class.canonicalName,
                "auto.offset.reset", "earliest")

        def readDriver = dag.newVertex("readDriver", StreamKafkaP.streamKafka(props, "driver"))
        def writeMap = dag.newVertex("writeMap", Processors.writeMap("juberDriverMap"))
        dag.edge(between(readDriver, writeMap))
        return dag
    }
}
