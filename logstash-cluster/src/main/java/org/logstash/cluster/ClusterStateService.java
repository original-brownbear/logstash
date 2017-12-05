package org.logstash.cluster;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.logstash.cluster.io.InetSocketAddressSerializer;
import org.logstash.cluster.raft.RaftNodeType;
import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

public final class ClusterStateService implements LsClusterService {

    private static final Logger LOGGER = LogManager.getLogger(ClusterStateService.class);

    private static final long TIMEOUT = 100L;

    private final Client esClient;

    private final String esIndex;

    private final DB database;

    private final HTreeMap.KeySet<InetSocketAddress> networkingPeers;

    private final Atomic.Long term;

    private final AtomicReference<RaftNodeType> nodeType = new AtomicReference<>(RaftNodeType.CANDIDATE);

    private final Atomic.String votedFor;

    private final CountDownLatch stopped = new CountDownLatch(1);

    private final Atomic.String identifier;

    ClusterStateService(final File stateFile, final Client esClient, final String esIndex) {
        this.esClient = esClient;
        this.esIndex = esIndex;
        database = DBMaker.fileDB(stateFile).make();
        term = database.atomicLong("raftTerm").createOrOpen();
        votedFor = database.atomicString("raftVotedFor").createOrOpen();
        identifier = database.atomicString("raftIdentifier").createOrOpen();
        identifier.compareAndSet(null, UUID.randomUUID().toString());
        networkingPeers = database.hashSet(
            "networkPeers", InetSocketAddressSerializer.INSTANCE
        ).createOrOpen();
    }

    public void registerPeer(final InetSocketAddress peer) {
        if (networkingPeers.add(peer)) {
            LOGGER.info("Registered new peer {}", peer);
        }
        database.commit();
    }

    public Collection<InetSocketAddress> peers() {
        return Collections.unmodifiableCollection(networkingPeers);
    }

    public String getIdentifier() {
        return identifier.get();
    }

    public long getTerm() {
        return term.get();
    }

    @Override
    public void run() {
        try {
            while (!stopped.await(TIMEOUT, TimeUnit.MILLISECONDS)) {
                database.commit();
            }
        } catch (final InterruptedException ex) {
            throw new IllegalStateException(ex);
        } finally {
            stopped.countDown();
        }
    }

    @Override
    public void close() {
        stop();
        awaitStop();
        LOGGER.info("Committing to local database.");
        database.commit();
        LOGGER.info("Closing local database.");
        database.close();
    }

    @Override
    public void stop() {
        stopped.countDown();
    }

    @Override
    public void awaitStop() {
        try {
            stopped.await();
        } catch (final InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private void convertToCandidate() {
        synchronized (nodeType) {
            nodeType.set(RaftNodeType.CANDIDATE);
            votedFor.set(this.identifier.get());
            term.incrementAndGet();
        }
    }

}
