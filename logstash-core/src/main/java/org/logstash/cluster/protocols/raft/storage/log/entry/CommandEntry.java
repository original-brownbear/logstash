package org.logstash.cluster.protocols.raft.storage.log.entry;

import org.logstash.cluster.protocols.raft.operation.RaftOperation;

/**
 * Stores a state machine command.
 * <p>
 * The {@code CommandEntry} is used to store an individual state machine command from an individual
 * client along with information relevant to sequencing the command in the server state machine.
 */
public class CommandEntry extends OperationEntry {
    public CommandEntry(long term, long timestamp, long session, long sequence, RaftOperation operation) {
        super(term, timestamp, session, sequence, operation);
    }
}
