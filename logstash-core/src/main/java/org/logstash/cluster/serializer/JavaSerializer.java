package org.logstash.cluster.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

public final class JavaSerializer implements Serializer {
    @Override
    public <T> byte[] encode(final T object) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final ObjectOutput oout = new ObjectOutputStream(baos)) {
            oout.writeObject(object);
            oout.flush();
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
        return baos.toByteArray();
    }

    @Override
    public <T> T decode(final byte[] bytes) {
        try (final ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return (T) oin.readObject();
        } catch (final IOException | ClassNotFoundException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
