package org.logstash.cluster.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import org.logstash.cluster.WorkerTask;

public final class TaskSerializer {

    private TaskSerializer() {
        // Utility Class
    }

    public static String serialize(final WorkerTask task) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final ObjectOutputStream oaos = new ObjectOutputStream(baos)) {
            oaos.writeObject(task);
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }

    public static WorkerTask deserialize(final String raw) {
        try (ObjectInputStream ois = new ObjectInputStream(
            new ByteArrayInputStream(Base64.getDecoder().decode(raw)))
        ) {
            return (WorkerTask) ois.readObject();
        } catch (final IOException | ClassNotFoundException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
