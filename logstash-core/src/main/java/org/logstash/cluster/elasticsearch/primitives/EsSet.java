package org.logstash.cluster.elasticsearch.primitives;

import java.util.Set;
import org.logstash.cluster.elasticsearch.EsClient;

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
        return map.asMap().keySet();
    }
}
