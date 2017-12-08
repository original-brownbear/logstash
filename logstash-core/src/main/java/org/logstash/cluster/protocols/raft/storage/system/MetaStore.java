package org.logstash.cluster.protocols.raft.storage.system;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.io.File;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.storage.RaftStorage;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.storage.StorageLevel;
import org.logstash.cluster.storage.buffer.Buffer;
import org.logstash.cluster.storage.buffer.FileBuffer;
import org.logstash.cluster.storage.buffer.HeapBuffer;

/**
 * Manages persistence of server configurations.
 * <p>
 * The server metastore is responsible for persisting server configurations according to the configured
 * {@link RaftStorage#storageLevel() storage level}. Each server persists their current {@link #loadTerm() term}
 * and last {@link #loadVote() vote} as is dictated by the Raft consensus algorithm. Additionally, the
 * metastore is responsible for storing the last know server {@link Configuration}, including cluster
 * membership.
 */
public class MetaStore implements AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(MetaStore.class);

    private final Serializer serializer;
    private final FileBuffer metadataBuffer;
    private final Buffer configurationBuffer;

    public MetaStore(RaftStorage storage, Serializer serializer) {
        this.serializer = Preconditions.checkNotNull(serializer, "serializer cannot be null");

        if (!(storage.directory().isDirectory() || storage.directory().mkdirs())) {
            throw new IllegalArgumentException(String.format("Can't create storage directory [%s].", storage.directory()));
        }

        // Note that for raft safety, irrespective of the storage level, <term, vote> metadata is always persisted on disk.
        File metaFile = new File(storage.directory(), String.format("%s.meta", storage.prefix()));
        metadataBuffer = FileBuffer.allocate(metaFile, 12);

        if (storage.storageLevel() == StorageLevel.MEMORY) {
            configurationBuffer = HeapBuffer.allocate(32);
        } else {
            File confFile = new File(storage.directory(), String.format("%s.conf", storage.prefix()));
            configurationBuffer = FileBuffer.allocate(confFile, 32);
        }
    }

    /**
     * Stores the current server term.
     * @param term The current server term.
     */
    public synchronized void storeTerm(long term) {
        LOGGER.trace("Store term {}", term);
        metadataBuffer.writeLong(0, term).flush();
    }

    /**
     * Loads the stored server term.
     * @return The stored server term.
     */
    public synchronized long loadTerm() {
        return metadataBuffer.readLong(0);
    }

    /**
     * Stores the last voted server.
     * @param vote The server vote.
     */
    public synchronized void storeVote(MemberId vote) {
        LOGGER.trace("Store vote {}", vote);
        metadataBuffer.writeString(8, vote != null ? vote.id() : null).flush();
    }

    /**
     * Loads the last vote for the server.
     * @return The last vote for the server.
     */
    public synchronized MemberId loadVote() {
        String id = metadataBuffer.readString(8);
        return id != null ? MemberId.from(id) : null;
    }

    /**
     * Stores the current cluster configuration.
     * @param configuration The current cluster configuration.
     */
    public synchronized void storeConfiguration(Configuration configuration) {
        LOGGER.trace("Store configuration {}", configuration);
        byte[] bytes = serializer.encode(configuration);
        configurationBuffer.position(0)
            .writeByte(1)
            .writeInt(bytes.length)
            .write(bytes);
        configurationBuffer.flush();
    }

    /**
     * Loads the current cluster configuration.
     * @return The current cluster configuration.
     */
    public synchronized Configuration loadConfiguration() {
        if (configurationBuffer.position(0).readByte() == 1) {
            return serializer.decode(configurationBuffer.readBytes(configurationBuffer.readInt()));
        }
        return null;
    }

    @Override
    public synchronized void close() {
        metadataBuffer.close();
        configurationBuffer.close();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).toString();
    }

}
