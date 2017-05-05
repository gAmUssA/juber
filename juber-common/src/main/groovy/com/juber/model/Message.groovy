package com.juber.model

import groovy.transform.Canonical
import groovy.transform.CompileStatic

@Canonical
@CompileStatic
class Message implements Serializable {
    String driver
    LngLat lngLat
    String rider
    String status
    long timestamp
    Route route
}
