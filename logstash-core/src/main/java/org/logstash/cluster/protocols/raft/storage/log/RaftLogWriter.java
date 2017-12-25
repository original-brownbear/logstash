package org.logstash.cluster.protocols.raft.storage.log;

import org.logstash.cluster.protocols.raft.storage.log.entry.RaftLogEntry;
import org.logstash.cluster.storage.journal.DelegatingJournalWriter;
import org.logstash.cluster.storage.journal.SegmentedJournalWriter;

/**
 * Raft log writer.
 */
public class RaftLogWriter extends DelegatingJournalWriter<RaftLogEntry> {
    private final SegmentedJournalWriter<RaftLogEntry> writer;
    private final RaftLog log;

    public RaftLogWriter(SegmentedJournalWriter<RaftLogEntry> writer, RaftLog log) {
        super(writer);
        this.writer = writer;
        this.log = log;
    }

    /**
     * Resets the head of the log to the given index.
     * @param index the index to which to reset the head of the log
     */
    public void reset(long index) {
        writer.reset(index);
    }

    /**
     * Commits entries up to the given index.
     * @param index The index up to which to commit entries.
     */
    public void commit(long index) {
        if (index > log.getCommitIndex()) {
            log.setCommitIndex(index);
            if (log.isFlushOnCommit()) {
                flush();
            }
        }
    }

    @Override
    public void truncate(long index) {
        if (index < log.getCommitIndex()) {
            throw new IndexOutOfBoundsException("Cannot truncate committed index: " + index);
        }
        super.truncate(index);
    }
}
