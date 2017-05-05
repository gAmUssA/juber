package com.juber.hasselhoff.jet

import com.hazelcast.config.XmlConfigBuilder
import com.hazelcast.jet.Jet
import com.hazelcast.jet.config.JetConfig

System.properties['hazelcast.logging.type'] = 'slf4j'
def jetConfig = new JetConfig()
def hazelcastXml = new XmlConfigBuilder().build()
jetConfig.setHazelcastConfig hazelcastXml

Jet.newJetInstance(jetConfig)