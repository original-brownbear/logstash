package org.logstash.cluster.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

/**
 * Interface for serialization of store artifacts.
 */
public interface Serializer {

    Serializer JAVA = new Serializer() {
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
    };

    /**
     * Creates a new Serializer instance from a Namespace.
     * @param namespace serializer namespace
     * @return Serializer instance
     */
    static Serializer using(final Namespace namespace) {
        return new Serializer() {
            @Override
            public <T> byte[] encode(final T object) {
                return namespace.serialize(object);
            }

            @Override
            public <T> T decode(final byte[] bytes) {
                return namespace.deserialize(bytes);
            }
        };
    }

    /**
     * Serialize the specified object.
     * @param object object to serialize.
     * @param <T> encoded type
     * @return serialized bytes.
     */
    <T> byte[] encode(T object);

    /**
     * Deserialize the specified bytes.
     * @param bytes byte array to deserialize.
     * @param <T> decoded type
     * @return deserialized object.
     */
    <T> T decode(byte[] bytes);

}
