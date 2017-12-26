package org.logstash.cluster.protocols.raft.storage.log;

import org.logstash.cluster.protocols.raft.storage.log.entry.RaftLogEntry;
import org.logstash.cluster.storage.journal.DelegatingJournalReader;
import org.logstash.cluster.storage.journal.SegmentedJournalReader;

/**
 * Raft log reader.
 */
public class RaftLogReader extends DelegatingJournalReader<RaftLogEntry> {

    private final SegmentedJournalReader<RaftLogEntry> reader;
    private final RaftLog log;
    private final RaftLogReader.Mode mode;

    public RaftLogReader(SegmentedJournalReader<RaftLogEntry> reader, RaftLog log, RaftLogReader.Mode mode) {
        super(reader);
        this.reader = reader;
        this.log = log;
        this.mode = mode;
    }

    /**
     * Returns the first index in the journal.
     * @return the first index in the journal
     */
    public long getFirstIndex() {
        return reader.getFirstIndex();
    }

    @Override
    public boolean hasNext() {
        if (mode == RaftLogReader.Mode.ALL) {
            return super.hasNext();
        }

        long nextIndex = getNextIndex();
        long commitIndex = log.getCommitIndex();
        return nextIndex <= commitIndex && super.hasNext();
    }

    /**
     * Raft log reader mode.
     */
    public enum Mode {

        /**
         * Reads all entries from the log.
         */
        ALL,

        /**
         * Reads committed entries from the log.
         */
        COMMITS,
    }
}
