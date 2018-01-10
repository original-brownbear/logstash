package org.logstash.cluster.elasticsearch;

import java.util.Collections;
import java.util.Set;

public final class EsSet {

    private final EsMap map;

    public EsSet(final EsClient client, final String name) {
        this.map = client.map(name);
    }

    public void add(final String value) {
        map.put(value, "");
    }

    public void remove(final String value) {
        map.remove(value);
    }

    public Set<String> asSet() {
        return Collections.emptySet();
    }
}
