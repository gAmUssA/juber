package com.juber.hasselhoff.jet

import com.hazelcast.client.config.XmlClientConfigBuilder
import com.hazelcast.jet.Jet

class JuberJetClient {
    static void main(String[] args) {
        def hazelcastClientXml = new XmlClientConfigBuilder().build()
        def jetClient = Jet.newJetClient(hazelcastClientXml)
        def job = jetClient.newJob(new Kafka2MapDAG().dag())
        job.execute().get()
    }
}
