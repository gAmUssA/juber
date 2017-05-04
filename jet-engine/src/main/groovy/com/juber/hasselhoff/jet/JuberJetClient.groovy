package com.juber.hasselhoff.jet

import com.hazelcast.client.config.XmlClientConfigBuilder
import com.hazelcast.jet.Jet

class JuberJetClient {
    static void main(String[] args) {
        def hazlecastClientXml = new XmlClientConfigBuilder().build()
        def jetClient = Jet.newJetClient(hazlecastClientXml)
        jetClient.newJob(new Kafka2MapDAG().dag())
    }
}
