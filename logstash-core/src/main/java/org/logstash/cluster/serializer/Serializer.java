package org.logstash.cluster.serializer;

/**
 * Interface for serialization of store artifacts.
 */
public interface Serializer {

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
