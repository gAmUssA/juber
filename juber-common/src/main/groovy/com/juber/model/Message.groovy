package com.juber.model

import groovy.transform.Canonical
import groovy.transform.CompileStatic

@Canonical
@CompileStatic
class Message {
    String driver
    LngLat lngLat
    String rider
    String status
    long timestamp
}
