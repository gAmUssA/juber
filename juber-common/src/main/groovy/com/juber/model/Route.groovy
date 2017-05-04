package com.juber.model

import groovy.transform.Canonical
import groovy.transform.CompileStatic

@Canonical
@CompileStatic
class Route {
    long distance
    long duration
    Step[] steps
    String geometry
    String summary
}
