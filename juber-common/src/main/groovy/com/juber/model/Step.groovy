package com.juber.model

import groovy.transform.Canonical
import groovy.transform.CompileStatic

@Canonical
@CompileStatic
class Step {
    long distance
    long duration
    String way_name
    String mode
    String direction
    long heading
    Instruction maneuver
}
