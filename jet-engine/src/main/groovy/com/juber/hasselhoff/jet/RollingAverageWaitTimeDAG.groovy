package com.juber.hasselhoff.jet

import com.hazelcast.jet.DAG
import com.hazelcast.jet.Distributed
import com.hazelcast.jet.Vertex
import com.hazelcast.jet.windowing.Frame
import com.hazelcast.jet.windowing.WindowDefinition
import com.juber.kafka.MessageSerializer
import com.juber.model.Message
import groovy.transform.CompileStatic
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

import static com.hazelcast.jet.DistributedFunctions.entryKey
import static com.hazelcast.jet.Edge.between
import static com.hazelcast.jet.Partitioner.HASH_CODE
import static com.hazelcast.jet.Processors.map
import static com.hazelcast.jet.Util.entry
import static com.hazelcast.jet.connector.kafka.StreamKafkaP.streamKafka
import static com.hazelcast.jet.windowing.PunctuationPolicies.cappingEventSeqLagAndRetention
import static com.hazelcast.jet.windowing.WindowDefinition.slidingWindowDef
import static com.hazelcast.jet.windowing.WindowOperations.counting
import static com.hazelcast.jet.windowing.WindowingProcessors.*
import static com.juber.kafka.KafkaServerStarter.props

@CompileStatic
class RollingAverageWaitTimeDAG implements Serializable {
    private static final int SLIDING_WINDOW_LENGTH_MILLIS = 30000
    private static final int SLIDE_STEP_MILLIS = 500
    public static final int MAX_LAG = 1000

    DAG dag() {
        WindowDefinition windowDef = slidingWindowDef(SLIDING_WINDOW_LENGTH_MILLIS, SLIDE_STEP_MILLIS)
        def formatString = "HH:mm:ss.SSS"


        def dag = new DAG()
        Properties props = props(
                "group.id", "group-" + Math.random(),
                "bootstrap.servers", "localhost:9092",
                "key.deserializer", StringDeserializer.class.canonicalName,
                "value.deserializer", MessageSerializer.class.canonicalName,
                "auto.offset.reset", "earliest")

        def readDriver = dag.newVertex("readDriver", streamKafka(props, "driver"))

        Vertex insertPunctuation = dag.newVertex("insert-punctuation",
                insertPunctuation({ Map.Entry<String, Message> e -> e.key.toLong()
                }, {
                    cappingEventSeqLagAndRetention(MAX_LAG, 100).throttleByFrame(windowDef)
                }))

        def keyExtractor = { Map.Entry<String, Message> e -> e.value.driver } as Distributed.Function
        Vertex groupByFrame = dag.newVertex("group-by-frame",
                groupByFrame(
                        keyExtractor,
                        { Map.Entry<String, Message> e -> e.key.toLong() } as Distributed.ToLongFunction<? super Object>, windowDef, counting()))


        def accumulator = entry(0, 0)

        Vertex slidingWin = dag.newVertex("sliding-window", slidingWindow(windowDef, counting()))

        Vertex formatOutput = dag.newVertex("format-output",
                map(new Distributed.Function<Frame<String, Long>, String>() {
                    @Override
                    String apply(Frame<String, Long> f) {
                        DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern(formatString)
                        final String format = String.format("%s %5s %4d",
                                timeFormat.format(Instant.ofEpochMilli(f.getSeq()).atZone(ZoneId.systemDefault())),
                                f.getKey(), f.getValue()
                        )
                        println("format = " + format)
                        return format
                    }
                }))
        dag
                .edge(between(readDriver, insertPunctuation).oneToMany())
                .edge(between(insertPunctuation, groupByFrame).partitioned(keyExtractor, HASH_CODE))
                .edge(between(groupByFrame, slidingWin).partitioned(entryKey())
                .distributed())
                .edge(between(slidingWin, formatOutput).oneToMany())
        //.edge(between(formatOutput, sink).oneToMany())
        dag
    }
}
