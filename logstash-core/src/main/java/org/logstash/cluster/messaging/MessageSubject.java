package org.logstash.cluster.messaging;

import com.google.common.base.Preconditions;
import java.util.Objects;

/**
 * Representation of a message subject.
 * Cluster messages have associated subjects that dictate how they get handled
 * on the receiving side.
 */
public final class MessageSubject {

    private final String name;

    public MessageSubject(String name) {
        this.name = Preconditions.checkNotNull(name);
    }

    // for serializer
    protected MessageSubject() {
        this.name = "";
    }

    /**
     * Returns the subject name.
     * @return the message subject name
     */
    public String name() {
        return name;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        MessageSubject that = (MessageSubject) obj;
        return Objects.equals(this.name, that.name);
    }

    @Override
    public String toString() {
        return name;
    }
}
