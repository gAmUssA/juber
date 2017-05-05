package com.juber.model

import groovy.transform.Canonical
import groovy.transform.CompileStatic

@Canonical
@CompileStatic
class Instruction implements Serializable {
    String instruction
    String type
    Location location
}
