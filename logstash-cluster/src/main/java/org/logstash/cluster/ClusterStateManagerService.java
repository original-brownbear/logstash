package org.logstash.cluster;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.jetbrains.annotations.NotNull;
import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

public final class ClusterStateManagerService implements LsClusterService {

    private static final Logger LOGGER = LogManager.getLogger(ClusterStateManagerService.class);

    private static final long TIMEOUT = 100L;

    private final Client esClient;

    private final DB database;

    private final HTreeMap.KeySet<InetSocketAddress> networkingPeers;

    private final Atomic.Long term;

    private final Atomic.String votedFor;

    private final CountDownLatch stopped = new CountDownLatch(1);

    ClusterStateManagerService(final File stateFile, final Client esClient) {
        this.esClient = esClient;
        database = DBMaker.fileDB(stateFile).make();
        term = database.atomicLong("raftTerm").createOrOpen();
        votedFor = database.atomicString("raftVotedFor").createOrOpen();
        networkingPeers = database.hashSet("networkPeers", new Serializer<InetSocketAddress>() {
            @Override
            public void serialize(@NotNull final DataOutput2 out,
                @NotNull final InetSocketAddress value) throws IOException {
                out.write(value.getAddress().getAddress());
                out.writeInt(value.getPort());
            }

            @Override
            public InetSocketAddress deserialize(@NotNull final DataInput2 input,
                final int available) throws IOException {
                final byte[] raw = new byte[available - Integer.BYTES];
                input.readFully(raw);
                return new InetSocketAddress(InetAddress.getByAddress(raw), input.readInt());
            }
        }).createOrOpen();
    }

    public Collection<InetSocketAddress> peers() {
        return Collections.unmodifiableCollection(networkingPeers);
    }

    @Override
    public void run() {
        try {
            while (stopped.getCount() > 0L) {
                TimeUnit.MILLISECONDS.sleep(TIMEOUT);
            }
        } catch (final InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void close() throws IOException {
        database.commit();
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
}
