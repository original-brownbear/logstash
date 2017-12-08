package org.logstash.cluster.protocols.raft;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.service.ServiceType;
import org.logstash.cluster.protocols.raft.session.RaftSessionMetadata;

/**
 * Raft metadata.
 */
public interface RaftMetadataClient {

    /**
     * Returns the current cluster leader.
     * @return The current cluster leader.
     */
    MemberId getLeader();

    /**
     * Returns the set of known members in the cluster.
     * @return The set of known members in the cluster.
     */
    default Collection<MemberId> getServers() {
        return getMembers();
    }

    /**
     * Returns the set of known members in the cluster.
     * @return The set of known members in the cluster.
     */
    Collection<MemberId> getMembers();

    /**
     * Returns a list of open sessions.
     * @return A completable future to be completed with a list of open sessions.
     */
    CompletableFuture<Set<RaftSessionMetadata>> getSessions();

    /**
     * Returns a list of open sessions of the given type.
     * @param serviceType the service type for which to return sessions
     * @return A completable future to be completed with a list of open sessions of the given type.
     */
    default CompletableFuture<Set<RaftSessionMetadata>> getSessions(String serviceType) {
        return getSessions(ServiceType.from(serviceType));
    }

    /**
     * Returns a list of open sessions of the given type.
     * @param serviceType the service type for which to return sessions
     * @return A completable future to be completed with a list of open sessions of the given type.
     */
    CompletableFuture<Set<RaftSessionMetadata>> getSessions(ServiceType serviceType);

    /**
     * Returns a list of open sessions for the given service.
     * @param serviceType the service type for which to return sessions
     * @param serviceName the service for which to return sessions
     * @return A completable future to be completed with a list of open sessions of the given type.
     */
    default CompletableFuture<Set<RaftSessionMetadata>> getSessions(String serviceType, String serviceName) {
        return getSessions(ServiceType.from(serviceType), serviceName);
    }

    /**
     * Returns a list of open sessions for the given service.
     * @param serviceType the service type for which to return sessions
     * @param serviceName the service for which to return sessions
     * @return A completable future to be completed with a list of open sessions of the given type.
     */
    CompletableFuture<Set<RaftSessionMetadata>> getSessions(ServiceType serviceType, String serviceName);

}
