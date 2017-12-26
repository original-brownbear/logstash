package org.logstash.cluster.protocols.raft;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

/**
 * Base type for Raft protocol errors.
 * <p>
 * Raft errors are passed on the wire in lieu of exceptions to reduce the overhead of serialization.
 * Each error is identifiable by an error ID which is used to serialize and deserialize errors.
 */
public class RaftError {
    private final RaftError.Type type;
    private final String message;

    public RaftError(RaftError.Type type, String message) {
        this.type = Preconditions.checkNotNull(type, "type cannot be null");
        this.message = message;
    }

    /**
     * Returns the error type.
     * @return The error type.
     */
    public RaftError.Type type() {
        return type;
    }

    /**
     * Returns the error message.
     * @return The error message.
     */
    public String message() {
        return message;
    }

    /**
     * Creates a new exception for the error.
     * @return The error exception.
     */
    public RaftException createException() {
        return type.createException(message);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("type", type)
            .add("message", message)
            .toString();
    }

    /**
     * Raft error types.
     */
    public enum Type {

        /**
         * No leader error.
         */
        NO_LEADER {
            @Override
            RaftException createException() {
                return createException("Failed to locate leader");
            }

            @Override
            RaftException createException(String message) {
                return message != null ? new RaftException.NoLeader(message) : createException();
            }
        },

        /**
         * Read application error.
         */
        QUERY_FAILURE {
            @Override
            RaftException createException() {
                return createException("Failed to obtain read quorum");
            }

            @Override
            RaftException createException(String message) {
                return message != null ? new RaftException.QueryFailure(message) : createException();
            }
        },

        /**
         * Write application error.
         */
        COMMAND_FAILURE {
            @Override
            RaftException createException() {
                return createException("Failed to obtain write quorum");
            }

            @Override
            RaftException createException(String message) {
                return message != null ? new RaftException.CommandFailure(message) : createException();
            }
        },

        /**
         * User application error.
         */
        APPLICATION_ERROR {
            @Override
            RaftException createException() {
                return createException("An application error occurred");
            }

            @Override
            RaftException createException(String message) {
                return message != null ? new RaftException.ApplicationException(message) : createException();
            }
        },

        /**
         * Illegal member state error.
         */
        ILLEGAL_MEMBER_STATE {
            @Override
            RaftException createException() {
                return createException("Illegal member state");
            }

            @Override
            RaftException createException(String message) {
                return message != null ? new RaftException.IllegalMemberState(message) : createException();
            }
        },

        /**
         * Unknown client error.
         */
        UNKNOWN_CLIENT {
            @Override
            RaftException createException() {
                return createException("Unknown client");
            }

            @Override
            RaftException createException(String message) {
                return message != null ? new RaftException.UnknownClient(message) : createException();
            }
        },

        /**
         * Unknown session error.
         */
        UNKNOWN_SESSION {
            @Override
            RaftException createException() {
                return createException("Unknown member session");
            }

            @Override
            RaftException createException(String message) {
                return message != null ? new RaftException.UnknownSession(message) : createException();
            }
        },

        /**
         * Unknown state machine error.
         */
        UNKNOWN_SERVICE {
            @Override
            RaftException createException() {
                return createException("Unknown state machine");
            }

            @Override
            RaftException createException(String message) {
                return message != null ? new RaftException.UnknownService(message) : createException();
            }
        },

        /**
         * Closed session error.
         */
        CLOSED_SESSION {
            @Override
            RaftException createException() {
                return createException("Closed session");
            }

            @Override
            RaftException createException(String message) {
                return message != null ? new RaftException.ClosedSession(message) : createException();
            }
        },

        /**
         * Internal error.
         */
        PROTOCOL_ERROR {
            @Override
            RaftException createException() {
                return createException("Failed to reach consensus");
            }

            @Override
            RaftException createException(String message) {
                return message != null ? new RaftException.ProtocolException(message) : createException();
            }
        },

        /**
         * Configuration error.
         */
        CONFIGURATION_ERROR {
            @Override
            RaftException createException() {
                return createException("Configuration failed");
            }

            @Override
            RaftException createException(String message) {
                return message != null ? new RaftException.ConfigurationException(message) : createException();
            }
        },

        /**
         * Unavailable service error.
         */
        UNAVAILABLE {
            @Override
            RaftException createException() {
                return createException("Service is unavailable");
            }

            @Override
            RaftException createException(String message) {
                return message != null ? new RaftException.Unavailable(message) : createException();
            }
        };

        /**
         * Creates an exception with a default message.
         * @return the exception
         */
        abstract RaftException createException();

        /**
         * Creates an exception with the given message.
         * @param message the exception message
         * @return the exception
         */
        abstract RaftException createException(String message);
    }
}
