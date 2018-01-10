package org.logstash.cluster.elasticsearch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.logstash.cluster.LogstashClusterConfig;

public final class EsClient implements AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(EsClient.class);

    private static final String ES_TYPE = "clusterState";

    private static final String ES_BOOTSTRAP_DOC = "bootstrapNodes";

    private static final String ES_JOB_SETTINGS_DOC = "jobSettings";

    private static final String ES_JOB_SETTINGS_FIELD = "settings";

    private static final String ES_NODES_FIELD = "nodes";

    private final Client client;

    private final LogstashClusterConfig config;

    public static EsClient create(final Client client,
        final LogstashClusterConfig defaults) throws ExecutionException, InterruptedException {
        return new EsClient(client, defaults);
    }

    private EsClient(final Client client, final LogstashClusterConfig config)
        throws ExecutionException, InterruptedException {
        this.client = client;
        this.config = config;
        ensureIndex();
    }

    public LogstashClusterConfig getConfig() {
        return config;
    }

    public EsMap map(final String name) {
        return new EsMap(this, name);
    }

    public EsLock lock(final String name) {
        return new EsLock(this, name);
    }

    public List<String> currentClusterNodes() {
        return Collections.unmodifiableList(new ArrayList<>(loadBootstrapNodes()));
    }

    public Map<String, String> currentJobSettings() {
        try {
            ensureIndex();
        } catch (final InterruptedException | ExecutionException ex) {
            throw new IllegalStateException(ex);
        }
        return deserializeJobSettingsResponse(getSettingsResponse());
    }

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

    public void publishLocalNode() {
        try {
            retryEsWrite(
                () -> {
                    final GetResponse existing = getNodesResponse();
                    final Collection<String> update = new HashSet<>();
                    if (existing.isExists()) {
                        update.addAll(deserializeNodesResponse(existing));
                        if (!update.add(config.localNode())) {
                            return;
                        }
                    }
                    LOGGER.info("Saving updated bootstrap node list {} to Elasticsearch.", String.join(" ,", update));
                    client.prepareUpdate().setId(ES_BOOTSTRAP_DOC).setDoc(
                        Collections.singletonMap(ES_NODES_FIELD, update)
                    ).setIndex(config.esIndex()).setType(ES_TYPE).setDocAsUpsert(true).setUpsert(
                        Collections.singletonMap(ES_NODES_FIELD, update)
                    ).execute().get();
                }
            );
        } catch (final InterruptedException | ExecutionException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public void close() {
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> deserializeJobSettingsResponse(final GetResponse response) {
        return (Map<String, String>) response.getSource().get(ES_JOB_SETTINGS_FIELD);
    }

    @SuppressWarnings("unchecked")
    private static Collection<String> deserializeNodesResponse(final GetResponse response) {
        return ((Collection<String>) response.getSource().get(ES_NODES_FIELD))
            .stream().map(EsClient::deserializeNode)
            .collect(Collectors.toList());
    }

    private Collection<String> loadBootstrapNodes() {
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
        retryEsWrite(() -> {
            if (!client.admin().indices().prepareExists(index).get().isExists()) {
                client.admin().indices().prepareCreate(index).execute().get();
            }
        });
        final String lnode = config.localNode();
        if (lnode != null) {
            publishLocalNode();
        }
        publishJobSettings(Collections.emptyMap());
    }

    private static String deserializeNode(final CharSequence data) {
        return data.toString();
    }

    private static void retryEsWrite(final EsClient.EsCall call)
        throws ExecutionException, InterruptedException {
        for (int i = 0; i < 5; ++i) {
            try {
                call.execute();
                return;
            } catch (final ExecutionException ignored) {
                LOGGER.warn("Concurrent update to ES detected, retrying in 1s.");
                TimeUnit.SECONDS.sleep(1L);
            }
        }
        call.execute();
    }

    @FunctionalInterface
    private interface EsCall {
        void execute() throws ExecutionException, InterruptedException;
    }
}
