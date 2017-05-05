package com.juber.hasselhoff.jet;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.stream.IStreamMap;
import com.juber.model.Message;

public class StreamHelper {

    public static double avg(JetInstance jetInstance) {
        IStreamMap<String, Message> map = jetInstance.getMap("juberDriverMap");
        return map.stream()
           .map(entry -> entry.getValue())
           .filter(value -> value.getRoute() != null)
           .map(value -> value.getRoute())
           .map(route -> new Pair<Long, Long>(route.getDistance(), route.getDuration()))
           .reduce((pair1, pair2) -> new Pair<Long, Long>(
                   pair1.component1() + pair2.component1(),
                   pair2.component2() + pair2.component2())
           )
           .map(pair -> ((double) pair.component1()) / pair.component2())
           .orElseThrow(() -> new RuntimeException());
    }

    public static class Pair<V1, V2> {
        private final V1 component1;
        private final V2 component2;

        public Pair(V1 component1, V2 component2) {
            this.component1 = component1;
            this.component2 = component2;
        }

        public V1 component1() {
            return component1;
        }

        public V2 component2() {
            return component2;
        }
    }

}
