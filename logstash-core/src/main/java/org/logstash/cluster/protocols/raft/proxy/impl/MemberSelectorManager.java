package org.logstash.cluster.protocols.raft.proxy.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.proxy.CommunicationStrategy;

/**
 * Cluster member selectors.
 */
public final class MemberSelectorManager {
    private final Set<MemberSelector> selectors = new CopyOnWriteArraySet<>();
    private volatile MemberId leader;
    private volatile Collection<MemberId> members = Collections.emptyList();

    /**
     * Returns the current cluster leader.
     * @return The current cluster leader.
     */
    public MemberId leader() {
        return leader;
    }

    /**
     * Returns the set of members in the cluster.
     * @return The set of members in the cluster.
     */
    public Collection<MemberId> members() {
        return members;
    }

    /**
     * Creates a new address selector.
     * @param selectionStrategy The server selection strategy.
     * @return A new address selector.
     */
    public MemberSelector createSelector(CommunicationStrategy selectionStrategy) {
        MemberSelector selector = new MemberSelector(leader, members, selectionStrategy, this);
        selectors.add(selector);
        return selector;
    }

    /**
     * Resets all child selectors.
     */
    public void resetAll() {
        selectors.forEach(MemberSelector::reset);
    }

    /**
     * Resets all child selectors.
     * @param leader The current cluster leader.
     * @param members The collection of all active members.
     */
    public void resetAll(MemberId leader, Collection<MemberId> members) {
        this.leader = leader;
        this.members = new LinkedList<>(members);
        selectors.forEach(s -> s.reset(leader, members));
    }

    /**
     * Removes the given selector.
     * @param selector The member selector to remove.
     */
    void remove(MemberSelector selector) {
        selectors.remove(selector);
    }

}
