package com.juber.model

import groovy.transform.Canonical
import groovy.transform.CompileStatic

@Canonical
@CompileStatic
class Location implements Serializable {
    String type
    double[] coordinates
}
