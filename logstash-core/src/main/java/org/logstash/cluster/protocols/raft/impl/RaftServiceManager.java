package org.logstash.cluster.protocols.raft.impl;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.protocols.raft.RaftException;
import org.logstash.cluster.protocols.raft.ReadConsistency;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.service.RaftService;
import org.logstash.cluster.protocols.raft.service.ServiceId;
import org.logstash.cluster.protocols.raft.service.ServiceType;
import org.logstash.cluster.protocols.raft.service.impl.DefaultServiceContext;
import org.logstash.cluster.protocols.raft.session.RaftSessionMetadata;
import org.logstash.cluster.protocols.raft.session.SessionId;
import org.logstash.cluster.protocols.raft.session.impl.RaftSessionContext;
import org.logstash.cluster.protocols.raft.storage.log.RaftLog;
import org.logstash.cluster.protocols.raft.storage.log.RaftLogReader;
import org.logstash.cluster.protocols.raft.storage.log.entry.CloseSessionEntry;
import org.logstash.cluster.protocols.raft.storage.log.entry.CommandEntry;
import org.logstash.cluster.protocols.raft.storage.log.entry.ConfigurationEntry;
import org.logstash.cluster.protocols.raft.storage.log.entry.InitializeEntry;
import org.logstash.cluster.protocols.raft.storage.log.entry.KeepAliveEntry;
import org.logstash.cluster.protocols.raft.storage.log.entry.MetadataEntry;
import org.logstash.cluster.protocols.raft.storage.log.entry.OpenSessionEntry;
import org.logstash.cluster.protocols.raft.storage.log.entry.QueryEntry;
import org.logstash.cluster.protocols.raft.storage.log.entry.RaftLogEntry;
import org.logstash.cluster.protocols.raft.storage.snapshot.Snapshot;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotReader;
import org.logstash.cluster.storage.journal.Indexed;
import org.logstash.cluster.utils.concurrent.Futures;
import org.logstash.cluster.utils.concurrent.ThreadContextFactory;

/**
 * Internal server state machine.
 * <p>
 * The internal state machine handles application of commands to the user provided {@link RaftService}
 * and keeps track of internal state like sessions and the various indexes relevant to log compaction.
 */
public class RaftServiceManager implements AutoCloseable {
    private static final Logger LOGGER = LogManager.getLogger(RaftServiceManager.class);
    private final RaftContext raft;
    private final ThreadContextFactory threadContextFactory;
    private final RaftLog log;
    private final RaftLogReader reader;

    public RaftServiceManager(final RaftContext raft, final ThreadContextFactory threadContextFactory) {
        this.raft = Preconditions.checkNotNull(raft, "state cannot be null");
        this.log = raft.getLog();
        this.reader = log.openReader(1, RaftLogReader.Mode.COMMITS);
        this.threadContextFactory = threadContextFactory;
    }

    /**
     * Applies all commits up to the given index.
     * <p>
     * Calls to this method are assumed not to expect a result. This allows some optimizations to be
     * made internally since linearizable events don't have to be waited to complete the command.
     * @param index The index up to which to apply commits.
     */
    public void applyAll(final long index) {
        // Don't attempt to apply indices that have already been applied.
        if (index > raft.getLastApplied()) {
            raft.getThreadContext().execute(() -> apply(index));
        }
    }

    /**
     * Applies the entry at the given index to the state machine.
     * <p>
     * Calls to this method are assumed to expect a result. This means linearizable session events
     * triggered by the application of the command at the given index will be awaited before completing
     * the returned future.
     * @param index The index to apply.
     * @return A completable future to be completed once the commit has been applied.
     */
    public <T> CompletableFuture<T> apply(final long index) {
        // Apply entries prior to this entry.
        while (reader.hasNext()) {
            final long nextIndex = reader.getNextIndex();

            // Validate that the next entry can be applied.
            final long lastApplied = raft.getLastApplied();
            if (nextIndex > lastApplied + 1 && nextIndex != reader.getFirstIndex()) {
                LOGGER.error("Cannot apply non-sequential index {} unless it's the first entry in the log: {}", nextIndex, reader.getFirstIndex());
                return Futures.exceptionalFuture(new IndexOutOfBoundsException("Cannot apply non-sequential index unless it's the first entry in the log"));
            } else if (nextIndex < lastApplied) {
                LOGGER.error("Cannot apply duplicate entry at index {}", nextIndex);
                return Futures.exceptionalFuture(new IndexOutOfBoundsException("Cannot apply duplicate entry at index " + nextIndex));
            }

            // If the next index is less than or equal to the given index, read and apply the entry.
            if (nextIndex < index) {
                final Indexed<RaftLogEntry> entry = reader.next();
                try {
                    apply(entry);
                    restoreIndex(entry.index());
                } catch (final Exception e) {
                    LOGGER.error("Failed to apply {}: {}", entry, e);
                } finally {
                    raft.setLastApplied(nextIndex);
                }
            }
            // If the next index is equal to the applied index, apply it and return the result.
            else if (nextIndex == index) {
                // Read the entry from the log. If the entry is non-null then apply it, otherwise
                // simply update the last applied index and return a null result.
                final Indexed<RaftLogEntry> entry = reader.next();
                try {
                    if (entry.index() != index) {
                        throw new IllegalStateException("inconsistent index applying entry " + index + ": " + entry);
                    }
                    final CompletableFuture<T> future = apply(entry);
                    restoreIndex(entry.index());
                    return future;
                } catch (final Exception e) {
                    LOGGER.error("Failed to apply {}: {}", entry, e);
                } finally {
                    raft.setLastApplied(nextIndex);
                }
            }
            // If the applied index has been passed, return a null result.
            else {
                raft.setLastApplied(nextIndex);
                return Futures.completedFuture(null);
            }
        }

        LOGGER.error("Cannot commit index " + index);
        return Futures.exceptionalFuture(new IndexOutOfBoundsException("Cannot commit index " + index));
    }

    /**
     * Applies an entry to the state machine.
     * <p>
     * Calls to this method are assumed to expect a result. This means linearizable session events
     * triggered by the application of the given entry will be awaited before completing the returned future.
     * @param entry The entry to apply.
     * @return A completable future to be completed with the result.
     */
    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<T> apply(final Indexed<? extends RaftLogEntry> entry) {
        LOGGER.trace("Applying {}", entry);
        if (entry.type() == QueryEntry.class) {
            return (CompletableFuture<T>) applyQuery(entry.cast());
        } else {
            if (entry.type() == CommandEntry.class) {
                return (CompletableFuture<T>) applyCommand(entry.cast());
            } else if (entry.type() == OpenSessionEntry.class) {
                return (CompletableFuture<T>) applyOpenSession(entry.cast());
            } else if (entry.type() == KeepAliveEntry.class) {
                return (CompletableFuture<T>) applyKeepAlive(entry.cast());
            } else if (entry.type() == CloseSessionEntry.class) {
                return (CompletableFuture<T>) applyCloseSession(entry.cast());
            } else if (entry.type() == MetadataEntry.class) {
                return (CompletableFuture<T>) applyMetadata(entry.cast());
            } else if (entry.type() == InitializeEntry.class) {
                return (CompletableFuture<T>) applyInitialize(entry.cast());
            } else if (entry.type() == ConfigurationEntry.class) {
                return (CompletableFuture<T>) applyConfiguration(entry.cast());
            }
        }
        return Futures.exceptionalFuture(new RaftException.ProtocolException("Unknown entry type"));
    }

    /**
     * Applies an initialize entry.
     * <p>
     * Initialize entries are used only at the beginning of a new leader's term to force the commitment of entries from
     * prior terms, therefore no logic needs to take place.
     */
    private CompletableFuture<Void> applyInitialize(final Indexed<InitializeEntry> entry) {
        for (final DefaultServiceContext service : raft.getServices()) {
            service.keepAliveSessions(entry.index(), entry.entry().timestamp());
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Applies a configuration entry to the internal state machine.
     * <p>
     * Configuration entries are applied to internal server state when written to the log. Thus, no significant
     * logic needs to take place in the handling of configuration entries. We simply release the previous configuration
     * entry since it was overwritten by a more recent committed configuration entry.
     */
    private CompletableFuture<Void> applyConfiguration(final Indexed<ConfigurationEntry> entry) {
        for (final DefaultServiceContext service : raft.getServices()) {
            service.keepAliveSessions(entry.index(), entry.entry().timestamp());
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Applies a session keep alive entry to the state machine.
     * <p>
     * Keep alive entries are applied to the internal state machine to reset the timeout for a specific session.
     * If the session indicated by the KeepAliveEntry is still held in memory, we mark the session as trusted,
     * indicating that the client has committed a keep alive within the required timeout. Additionally, we check
     * all other sessions for expiration based on the timestamp provided by this KeepAliveEntry. Note that sessions
     * are never completely expired via this method. Leaders must explicitly commit an UnregisterEntry to expire
     * a session.
     * <p>
     * When a KeepAliveEntry is committed to the internal state machine, two specific fields provided in the entry
     * are used to update server-side session state. The {@code commandSequence} indicates the highest command for
     * which the session has received a successful response in the proper sequence. By applying the {@code commandSequence}
     * to the server session, we clear command output held in memory up to that point. The {@code eventVersion} indicates
     * the index up to which the client has received event messages in sequence for the session. Applying the
     * {@code eventVersion} to the server-side session results in events up to that index being removed from memory
     * as they were acknowledged by the client. It's essential that both of these fields be applied via entries committed
     * to the Raft log to ensure they're applied on all servers in sequential order.
     * <p>
     * Keep alive entries are retained in the log until the next time the client sends a keep alive entry or until the
     * client's session is expired. This ensures for sessions that have long timeouts, keep alive entries cannot be cleaned
     * from the log before they're replicated to some servers.
     */
    private CompletableFuture<long[]> applyKeepAlive(final Indexed<KeepAliveEntry> entry) {
        // Store the session/command/event sequence and event index instead of acquiring a reference to the entry.
        final long[] sessionIds = entry.entry().sessionIds();
        final long[] commandSequences = entry.entry().commandSequenceNumbers();
        final long[] eventIndexes = entry.entry().eventIndexes();

        // Iterate through session identifiers and keep sessions alive.
        final List<Long> successfulSessionIds = new ArrayList<>(sessionIds.length);
        final List<CompletableFuture<Void>> futures = new ArrayList<>(sessionIds.length);
        for (int i = 0; i < sessionIds.length; i++) {
            final long sessionId = sessionIds[i];
            final long commandSequence = commandSequences[i];
            final long eventIndex = eventIndexes[i];

            final RaftSessionContext session = raft.getSessions().getSession(sessionId);
            if (session != null) {
                final CompletableFuture<Void> future = session.getService().keepAlive(entry.index(), entry.entry().timestamp(), session, commandSequence, eventIndex)
                    .thenApply(succeeded -> {
                        if (succeeded) {
                            synchronized (successfulSessionIds) {
                                successfulSessionIds.add(sessionId);
                            }
                        }
                        return null;
                    });
                futures.add(future);
            }
        }

        // Iterate through services and complete keep-alives, causing sessions to be expired if necessary.
        for (final DefaultServiceContext service : raft.getServices()) {
            service.completeKeepAlive(entry.index(), entry.entry().timestamp());
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]))
            .thenApply(v -> {
                synchronized (successfulSessionIds) {
                    return Longs.toArray(successfulSessionIds);
                }
            });
    }

    /**
     * Applies an open session entry to the state machine.
     */
    private CompletableFuture<Long> applyOpenSession(final Indexed<OpenSessionEntry> entry) {
        // Get the state machine executor or create one if it doesn't already exist.
        final DefaultServiceContext service = getOrInitializeService(
            ServiceId.from(entry.index()),
            ServiceType.from(entry.entry().serviceType()),
            entry.entry().serviceName());
        if (service == null) {
            return Futures.exceptionalFuture(new RaftException.UnknownService("Unknown service type " + entry.entry().serviceType()));
        }

        final SessionId sessionId = SessionId.from(entry.index());
        final RaftSessionContext session = new RaftSessionContext(
            sessionId,
            MemberId.from(entry.entry().memberId()),
            entry.entry().serviceName(),
            ServiceType.from(entry.entry().serviceType()),
            entry.entry().readConsistency(),
            entry.entry().minTimeout(),
            entry.entry().maxTimeout(),
            service,
            raft,
            threadContextFactory);
        raft.getSessions().registerSession(session);
        return service.openSession(entry.index(), entry.entry().timestamp(), session);
    }

    /**
     * Gets or initializes a service context.
     */
    private DefaultServiceContext getOrInitializeService(final ServiceId serviceId, final ServiceType serviceType, final String serviceName) {
        // Get the state machine executor or create one if it doesn't already exist.
        DefaultServiceContext service = raft.getServices().getService(serviceName);
        if (service == null) {
            service = initializeService(serviceId, serviceType, serviceName);
        }
        return service;
    }

    /**
     * Initializes a new service.
     */
    private DefaultServiceContext initializeService(final ServiceId serviceId, final ServiceType serviceType, final String serviceName) {
        final Supplier<RaftService> serviceFactory = raft.getServiceFactories().getFactory(serviceType.id());
        if (serviceFactory == null) {
            LOGGER.warn("Unknown service type: {}", serviceType);
            return null;
        }

        final DefaultServiceContext oldService = raft.getServices().getService(serviceName);
        final DefaultServiceContext service = new DefaultServiceContext(
            serviceId,
            serviceName,
            serviceType,
            serviceFactory.get(),
            raft,
            threadContextFactory);
        raft.getServices().registerService(service);

        // If a service with this name was already registered, remove all of its sessions.
        if (oldService != null) {
            raft.getSessions().removeSessions(oldService.serviceId());
        }
        return service;
    }

    /**
     * Applies a close session entry to the state machine.
     */
    private CompletableFuture<Void> applyCloseSession(final Indexed<CloseSessionEntry> entry) {
        final RaftSessionContext session = raft.getSessions().getSession(entry.entry().session());

        // If the server session is null, the session either never existed or already expired.
        if (session == null) {
            LOGGER.warn("Unknown session: " + entry.entry().session());
            return Futures.exceptionalFuture(new RaftException.UnknownSession("Unknown session: " + entry.entry().session()));
        }

        // Get the state machine executor associated with the session and unregister the session.
        final DefaultServiceContext service = session.getService();
        return service.closeSession(entry.index(), entry.entry().timestamp(), session, entry.entry().expired());
    }

    /**
     * Applies a metadata entry to the state machine.
     */
    private CompletableFuture<MetadataResult> applyMetadata(final Indexed<MetadataEntry> entry) {
        // If the session ID is non-zero, read the metadata for the associated state machine.
        if (entry.entry().session() > 0) {
            final RaftSessionContext session = raft.getSessions().getSession(entry.entry().session());

            // If the session is null, return an UnknownSessionException.
            if (session == null) {
                LOGGER.warn("Unknown session: " + entry.entry().session());
                return Futures.exceptionalFuture(new RaftException.UnknownSession("Unknown session: " + entry.entry().session()));
            }

            final Set<RaftSessionMetadata> sessions = new HashSet<>();
            for (final RaftSessionContext s : raft.getSessions().getSessions()) {
                if (s.serviceName().equals(session.serviceName())) {
                    sessions.add(new RaftSessionMetadata(s.sessionId().id(), s.serviceName(), s.serviceType().id()));
                }
            }
            return CompletableFuture.completedFuture(new MetadataResult(sessions));
        } else {
            final Set<RaftSessionMetadata> sessions = new HashSet<>();
            for (final RaftSessionContext session : raft.getSessions().getSessions()) {
                sessions.add(new RaftSessionMetadata(session.sessionId().id(), session.serviceName(), session.serviceType().id()));
            }
            return CompletableFuture.completedFuture(new MetadataResult(sessions));
        }
    }

    /**
     * Applies a command entry to the state machine.
     * <p>
     * Command entries result in commands being executed on the user provided {@link RaftService} and a
     * response being sent back to the client by completing the returned future. All command responses are
     * cached in the command's {@link RaftSessionContext} for fault tolerance. In the event that the same command
     * is applied to the state machine more than once, the original response will be returned.
     * <p>
     * Command entries are written with a sequence number. The sequence number is used to ensure that
     * commands are applied to the state machine in sequential order. If a command entry has a sequence
     * number that is less than the next sequence number for the session, that indicates that it is a
     * duplicate of a command that was already applied. Otherwise, commands are assumed to have been
     * received in sequential order. The reason for this assumption is because leaders always sequence
     * commands as they're written to the log, so no sequence number will be skipped.
     */
    private CompletableFuture<OperationResult> applyCommand(final Indexed<CommandEntry> entry) {
        // First check to ensure that the session exists.
        final RaftSessionContext session = raft.getSessions().getSession(entry.entry().session());

        // If the session is null, return an UnknownSessionException. Commands applied to the state machine must
        // have a session. We ensure that session register/unregister entries are not compacted from the log
        // until all associated commands have been cleaned.
        if (session == null) {
            LOGGER.warn("Unknown session: " + entry.entry().session());
            return Futures.exceptionalFuture(new RaftException.UnknownSession("unknown session: " + entry.entry().session()));
        }

        // Increment the load counter to avoid snapshotting under high load.
        raft.getLoadMonitor().recordEvent();

        // Execute the command using the state machine associated with the session.
        return session.getService()
            .executeCommand(
                entry.index(),
                entry.entry().sequenceNumber(),
                entry.entry().timestamp(),
                session,
                entry.entry().operation());
    }

    /**
     * Applies a query entry to the state machine.
     * <p>
     * Query entries are applied to the user {@link RaftService} for read-only operations.
     * Because queries are read-only, they may only be applied on a single server in the cluster,
     * and query entries do not go through the Raft log. Thus, it is critical that measures be taken
     * to ensure clients see a consistent view of the cluster event when switching servers. To do so,
     * clients provide a sequence and version number for each query. The sequence number is the order
     * in which the query was sent by the client. Sequence numbers are shared across both commands and
     * queries. The version number indicates the last index for which the client saw a command or query
     * response. In the event that the lastApplied index of this state machine does not meet the provided
     * version number, we wait for the state machine to catch up before applying the query. This ensures
     * clients see state progress monotonically even when switching servers.
     * <p>
     * Because queries may only be applied on a single server in the cluster they cannot result in the
     * publishing of session events. Events require commands to be written to the Raft log to ensure
     * fault-tolerance and consistency across the cluster.
     */
    private CompletableFuture<OperationResult> applyQuery(final Indexed<QueryEntry> entry) {
        final RaftSessionContext session = raft.getSessions().getSession(entry.entry().session());

        // If the session is null then that indicates that the session already timed out or it never existed.
        // Return with an UnknownSessionException.
        if (session == null) {
            LOGGER.warn("Unknown session: " + entry.entry().session());
            return Futures.exceptionalFuture(new RaftException.UnknownSession("unknown session " + entry.entry().session()));
        }

        // Execute the query using the state machine associated with the session.
        return session.getService()
            .executeQuery(
                entry.index(),
                entry.entry().sequenceNumber(),
                entry.entry().timestamp(),
                session,
                entry.entry().operation());
    }

    /**
     * Prepares sessions for the given index.
     * @param index the index for which to prepare sessions
     */
    private void restoreIndex(final long index) {
        final Collection<Snapshot> snapshots = raft.getSnapshotStore().getSnapshotsByIndex(index);

        // If snapshots exist for the prior index, iterate through snapshots and populate services/sessions.
        if (snapshots != null) {
            for (final Snapshot snapshot : snapshots) {
                try (SnapshotReader reader = snapshot.openReader()) {
                    restoreService(reader);
                }
            }
        }
    }

    /**
     * Restores the service associated with the given snapshot.
     * @param reader the snapshot reader
     */
    private void restoreService(final SnapshotReader reader) {
        final ServiceId serviceId = ServiceId.from(reader.readLong());
        final ServiceType serviceType = ServiceType.from(reader.readString());
        final String serviceName = reader.readString();

        // Get or create the service associated with the snapshot.
        LOGGER.debug("Restoring service {} {}", serviceId, serviceName);
        final DefaultServiceContext service = initializeService(serviceId, serviceType, serviceName);
        if (service == null) {
            return;
        }

        restoreSessions(reader, service);
    }

    /**
     * Restores the sessions associated with the given snapshot and service.
     * @param reader the snapshot reader
     * @param service the restored service
     */
    private void restoreSessions(final SnapshotReader reader, final DefaultServiceContext service) {
        // Read and create sessions from the snapshot.
        final int sessionCount = reader.readInt();
        for (int i = 0; i < sessionCount; i++) {
            restoreSession(reader, service);
        }
    }

    /**
     * Restores the next session in the given snapshot for the given service.
     * @param reader the snapshot reader
     * @param service the restored service
     */
    private void restoreSession(final SnapshotReader reader, final DefaultServiceContext service) {
        final SessionId sessionId = SessionId.from(reader.readLong());
        LOGGER.trace("Restoring session {} for {}", sessionId, service.serviceName());
        final MemberId node = MemberId.from(reader.readString());
        final ReadConsistency readConsistency = ReadConsistency.valueOf(reader.readString());
        final long minTimeout = reader.readLong();
        final long maxTimeout = reader.readLong();
        final long sessionTimestamp = reader.readLong();
        final RaftSessionContext session = new RaftSessionContext(
            sessionId,
            node,
            service.serviceName(),
            service.serviceType(),
            readConsistency,
            minTimeout,
            maxTimeout,
            service,
            raft,
            threadContextFactory);
        session.setLastUpdated(sessionTimestamp);
        session.setRequestSequence(reader.readLong());
        session.setCommandSequence(reader.readLong());
        session.setEventIndex(reader.readLong());
        session.setLastCompleted(reader.readLong());
        session.setLastApplied(reader.snapshot().index());
        raft.getSessions().registerSession(session);
    }

    @Override
    public void close() {
        // Don't close the thread context here since state machines can be reused.
    }
}
