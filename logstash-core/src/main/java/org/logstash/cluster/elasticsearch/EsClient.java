package org.logstash.cluster.elasticsearch;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.logstash.cluster.LogstashClusterConfig;
import org.logstash.cluster.elasticsearch.primitives.EsLock;
import org.logstash.cluster.elasticsearch.primitives.EsMap;
import org.logstash.cluster.elasticsearch.primitives.EsQueue;
import org.logstash.cluster.elasticsearch.primitives.EsSet;
import org.logstash.cluster.execution.LeaderElectionAction;
import org.logstash.cluster.state.Partition;

public final class EsClient implements AutoCloseable {

    private static final String ES_BOOTSTRAP_DOC = "bootstrapNodes";

    private static final String ES_JOB_SETTINGS_DOC = "jobSettings";

    private final LsEsRestClient client;

    public static EsClient create(final LogstashClusterConfig defaults) {
        return new EsClient(
            new RestHighLevelClient(
                RestClient.builder(defaults.esHosts().toArray(new HttpHost[0]))
            ), defaults
        );
    }

    private EsClient(final RestHighLevelClient client, final LogstashClusterConfig config) {
        this.client = new LsEsRestClient(config, client);
    }

    public LogstashClusterConfig getConfig() {
        return client.getConfig();
    }

    public EsSet set(final String name) {
        return new EsSet(this, name);
    }

    public EsMap map(final String name) {
        return EsMap.create(client, name);
    }

    public EsQueue taskQueue() {
        return EsQueue.create(client);
    }

    public EsLock lock(final String name) {
        return EsLock.create(client, name);
    }

    public Collection<Partition> getPartitions() {
        return Partition.fromMap(map(LeaderElectionAction.PARTITION_MAP_DOC));
    }

    public Collection<String> currentClusterNodes() {
        return new EsSet(this, ES_BOOTSTRAP_DOC).asSet();
    }

    public Map<String, Object> currentJobSettings() {
        return map(ES_JOB_SETTINGS_DOC).asMap();
    }

    public void publishJobSettings(final Map<String, Object> settings) {
        map(ES_JOB_SETTINGS_DOC).putAll(settings);
    }

    public void publishLocalNode() {
        set(ES_BOOTSTRAP_DOC).add(client.getConfig().localNode());
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

}
