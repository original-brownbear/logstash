package org.logstash.cluster.protocols.raft.proxy.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.proxy.CommunicationStrategy;

/**
 * Cluster member selector.
 */
public final class MemberSelector implements Iterator<MemberId>, AutoCloseable {

    private final MemberSelectorManager selectors;
    private final CommunicationStrategy strategy;
    private MemberId leader;
    private Collection<MemberId> members;
    private volatile MemberId selection;
    private Collection<MemberId> selections;
    private Iterator<MemberId> selectionsIterator;

    public MemberSelector(MemberId leader, Collection<MemberId> members, CommunicationStrategy strategy, MemberSelectorManager selectors) {
        this.leader = leader;
        this.members = Preconditions.checkNotNull(members, "servers cannot be null");
        this.strategy = Preconditions.checkNotNull(strategy, "strategy cannot be null");
        this.selectors = Preconditions.checkNotNull(selectors, "selectors cannot be null");
        this.selections = strategy.selectConnections(leader, new ArrayList<>(members));
    }

    /**
     * Returns the address selector state.
     * @return The address selector state.
     */
    public MemberSelector.State state() {
        if (selectionsIterator == null) {
            return MemberSelector.State.RESET;
        } else if (hasNext()) {
            return MemberSelector.State.ITERATE;
        } else {
            return MemberSelector.State.COMPLETE;
        }
    }

    @Override
    public boolean hasNext() {
        return selectionsIterator == null ? !selections.isEmpty() : selectionsIterator.hasNext();
    }

    @Override
    public MemberId next() {
        if (selectionsIterator == null) {
            selectionsIterator = selections.iterator();
        }
        MemberId selection = selectionsIterator.next();
        this.selection = selection;
        return selection;
    }

    /**
     * Returns the current address selection.
     * @return The current address selection.
     */
    public MemberId current() {
        return selection;
    }

    /**
     * Returns the current selector leader.
     * @return The current selector leader.
     */
    public MemberId leader() {
        return leader;
    }

    /**
     * Returns the current set of servers.
     * @return The current set of servers.
     */
    public Collection<MemberId> members() {
        return members;
    }

    /**
     * Resets the member iterator.
     * @return The member selector.
     */
    public MemberSelector reset() {
        if (selectionsIterator != null) {
            this.selections = strategy.selectConnections(leader, new ArrayList<>(members));
            this.selectionsIterator = null;
        }
        return this;
    }

    /**
     * Resets the connection leader and members.
     * @param members The collection of members.
     * @return The member selector.
     */
    public MemberSelector reset(MemberId leader, Collection<MemberId> members) {
        if (changed(leader, members)) {
            this.leader = leader;
            this.members = members;
            this.selections = strategy.selectConnections(leader, new ArrayList<>(members));
            this.selectionsIterator = null;
        }
        return this;
    }

    /**
     * Returns a boolean value indicating whether the selector state would be changed by the given members.
     */
    private boolean changed(MemberId leader, Collection<MemberId> servers) {
        Preconditions.checkNotNull(servers, "servers");
        Preconditions.checkArgument(!servers.isEmpty(), "servers cannot be empty");
        if (this.leader != null && leader == null) {
            return true;
        } else if (this.leader == null && leader != null) {
            Preconditions.checkArgument(servers.contains(leader), "leader must be present in the servers list");
            return true;
        } else if (this.leader != null && !this.leader.equals(leader)) {
            Preconditions.checkArgument(servers.contains(leader), "leader must be present in the servers list");
            return true;
        } else if (!matches(this.members, servers)) {
            return true;
        }
        return false;
    }

    /**
     * Returns a boolean value indicating whether the servers in the first list match the servers in the second list.
     */
    private static boolean matches(Collection<MemberId> left, Collection<MemberId> right) {
        if (left.size() != right.size())
            return false;

        for (MemberId address : left) {
            if (!right.contains(address)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() {
        selectors.remove(this);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("strategy", strategy)
            .toString();
    }

    /**
     * 1
     * Address selector state.
     */
    public enum State {

        /**
         * Indicates that the selector has been reset.
         */
        RESET,

        /**
         * Indicates that the selector is being iterated.
         */
        ITERATE,

        /**
         * Indicates that selector iteration is complete.
         */
        COMPLETE

    }

}
