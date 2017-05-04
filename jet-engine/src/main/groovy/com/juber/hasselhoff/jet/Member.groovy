package com.juber.hasselhoff.jet

import com.hazelcast.config.XmlConfigBuilder
import com.hazelcast.jet.Jet
import com.hazelcast.jet.config.JetConfig

def jetConfig = new JetConfig()
def hazelcastXml = new XmlConfigBuilder().build()
jetConfig.setHazelcastConfig hazelcastXml

Jet.newJetInstance(jetConfig)