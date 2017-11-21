package org.logstash.cluster;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

final class EsBlockstore implements BlockStore {

    private final DistributedClusterContext clusterContext;

    private final HttpHost[] esHosts;

    private transient RestClient esClient;

    EsBlockstore(final DistributedClusterContext cluster, final HttpHost... esHosts) {
        clusterContext = cluster;
        this.esHosts = esHosts;
        esClient = RestClient.builder(this.esHosts).build();
    }

    @Override
    public void store(final BlockId key, final ByteBuffer buffer) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long load(final BlockId key, final ByteBuffer buffer) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(final BlockId key) throws IOException {
        throw new UnsupportedOperationException();
    }
}
