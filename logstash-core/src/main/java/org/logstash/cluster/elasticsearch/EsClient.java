package org.logstash.cluster.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.logstash.cluster.ClusterConfigProvider;
import org.logstash.cluster.LogstashClusterConfig;
import org.logstash.cluster.cluster.Node;
import org.logstash.cluster.cluster.NodeId;
import org.logstash.cluster.cluster.impl.DefaultNode;
import org.logstash.cluster.messaging.Endpoint;

public final class EsClient implements ClusterConfigProvider {

    private static final Logger LOGGER = LogManager.getLogger(EsClient.class);

    private static final String ES_TYPE = "clusterState";

    private static final String ES_BOOTSTRAP_DOC = "bootstrapNodes";

    private static final String ES_JOB_SETTINGS_DOC = "jobSettings";

    private static final String ES_JOB_SETTINGS_FIELD = "settings";

    private static final String ES_NODES_FIELD = "nodes";

    private static final Pattern BOOTSTRAP_NODES_PATTERN = Pattern.compile("\\|");

    private final Client client;

    private final LogstashClusterConfig config;

    public EsClient(final Client client, final LogstashClusterConfig config)
        throws ExecutionException, InterruptedException {
        this.client = client;
        this.config = config;
        ensureIndex();
    }

    @Override
    public LogstashClusterConfig currentClusterConfig() {
        return config.withBootstrap(loadBootstrapNodes());
    }

    @Override
    public Map<String, String> currentJobSettings() {
        try {
            ensureIndex();
        } catch (final InterruptedException | ExecutionException ex) {
            throw new IllegalStateException(ex);
        }
        return deserializeJobSettingsResponse(getSettingsResponse());
    }

    @Override
    public void publishJobSettings(final Map<String, String> settings) {
        final GetResponse existing = getSettingsResponse();
        final Map<String, String> existingSettings;
        if (existing.isExists()) {
            existingSettings = deserializeJobSettingsResponse(existing);
        } else {
            existingSettings = new HashMap<>();
        }
        existingSettings.putAll(settings);
        try {
            client.prepareUpdate().setId(ES_JOB_SETTINGS_DOC).setDoc(
                Collections.singletonMap(ES_JOB_SETTINGS_FIELD, existingSettings)
            ).setIndex(config.esIndex()).setType(ES_TYPE).setDocAsUpsert(true).setUpsert(
                Collections.singletonMap(ES_JOB_SETTINGS_FIELD, existingSettings)
            ).execute().get();
        } catch (final InterruptedException | ExecutionException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void publishBootstrapNodes(final Collection<Node> nodes) {
        final GetResponse existing = getNodesResponse();
        final Collection<Node> update = new HashSet<>();
        if (existing.isExists()) {
            update.addAll(deserializeNodesResponse(existing));
            if (!(update.addAll(nodes) || update.add(config.localNode()))) {
                return;
            }
        }
        final Collection<String> serialized = update.stream().map(EsClient::serializeNode)
            .collect(Collectors.toList());
        try {
            LOGGER.info("Saving updated bootstrap node list to Elasticsearch.");
            client.prepareUpdate().setId(ES_BOOTSTRAP_DOC).setDoc(
                Collections.singletonMap(ES_NODES_FIELD, serialized)
            ).setIndex(config.esIndex()).setType(ES_TYPE).setDocAsUpsert(true).setUpsert(
                Collections.singletonMap(ES_NODES_FIELD, serialized)
            ).execute().get();
        } catch (final InterruptedException | ExecutionException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void close() {
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> deserializeJobSettingsResponse(final GetResponse response) {
        return (Map<String, String>) response.getSource().get(ES_JOB_SETTINGS_FIELD);
    }

    @SuppressWarnings("unchecked")
    private static Collection<Node> deserializeNodesResponse(final GetResponse response) {
        return ((Collection<String>) response.getSource().get(ES_NODES_FIELD))
            .stream().map(EsClient::deserializeNode)
            .collect(Collectors.toList());
    }

    private Collection<Node> loadBootstrapNodes() {
        try {
            ensureIndex();
            return deserializeNodesResponse(getNodesResponse());
        } catch (final InterruptedException | ExecutionException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private GetResponse getSettingsResponse() {
        return client.prepareGet()
            .setIndex(config.esIndex())
            .setType(ES_TYPE)
            .setId(ES_JOB_SETTINGS_DOC).setRealtime(true).setRefresh(true)
            .execute().actionGet();
    }

    private GetResponse getNodesResponse() {
        return client.prepareGet()
            .setIndex(config.esIndex())
            .setType(ES_TYPE)
            .setId(ES_BOOTSTRAP_DOC).setRealtime(true).setRefresh(true)
            .execute().actionGet();
    }

    private void ensureIndex() throws ExecutionException, InterruptedException {
        final String index = config.esIndex();
        if (!client.admin().indices().prepareExists(index).get().isExists()) {
            client.admin().indices().prepareCreate(index).execute().get();
        }
        final Node lnode = config.localNode();
        if (lnode != null) {
            publishBootstrapNodes(Collections.singleton(lnode));
        }
        publishJobSettings(Collections.emptyMap());
    }

    private static String serializeNode(final Node node) {
        return String.format(
            "%s|%s|%d", node.id(), node.endpoint().host().getHostAddress(), node.endpoint().port()
        );
    }

    private static DefaultNode deserializeNode(final CharSequence data) {
        final String[] parts = BOOTSTRAP_NODES_PATTERN.split(data);
        try {
            return new DefaultNode(
                NodeId.from(parts[0]),
                new Endpoint(InetAddress.getByName(parts[1]), Integer.parseInt(parts[2]))
            );
        } catch (final UnknownHostException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
