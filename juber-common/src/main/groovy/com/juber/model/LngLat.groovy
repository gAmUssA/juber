package com.juber.model

import groovy.transform.Canonical
import groovy.transform.CompileStatic;

@Canonical
@CompileStatic
class LngLat implements Serializable {
    double lat
    double lng
}
