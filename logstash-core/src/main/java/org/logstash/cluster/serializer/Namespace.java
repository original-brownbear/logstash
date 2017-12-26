package org.logstash.cluster.serializer;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Serializer namespace.
 */
public interface Namespace {

    int DEFAULT_BUFFER_SIZE = 4096;
    int MAX_BUFFER_SIZE = 100 * 1000 * 1000;

    /**
     * Serializes given object to byte array using Kryo instance in pool.
     * <p>
     * Note: Serialized bytes must be smaller than {@link #MAX_BUFFER_SIZE}.
     * @param obj Object to serialize
     * @return serialized bytes
     */
    default byte[] serialize(final Object obj) {
        return serialize(obj, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Serializes given object to byte array using Kryo instance in pool.
     * @param obj Object to serialize
     * @param bufferSize maximum size of serialized bytes
     * @return serialized bytes
     */
    byte[] serialize(Object obj, int bufferSize);

    /**
     * Serializes given object to byte buffer using Kryo instance in pool.
     * @param obj Object to serialize
     * @param buffer to write to
     */
    void serialize(Object obj, ByteBuffer buffer);

    /**
     * Serializes given object to OutputStream using Kryo instance in pool.
     * @param obj Object to serialize
     * @param stream to write to
     */
    default void serialize(final Object obj, final OutputStream stream) {
        serialize(obj, stream, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Serializes given object to OutputStream using Kryo instance in pool.
     * @param obj Object to serialize
     * @param stream to write to
     * @param bufferSize size of the buffer in front of the stream
     */
    void serialize(Object obj, OutputStream stream, int bufferSize);

    /**
     * Deserializes given byte array to Object using Kryo instance in pool.
     * @param bytes serialized bytes
     * @param <T> deserialized Object type
     * @return deserialized Object
     */
    <T> T deserialize(byte[] bytes);

    /**
     * Deserializes given byte buffer to Object using Kryo instance in pool.
     * @param buffer input with serialized bytes
     * @param <T> deserialized Object type
     * @return deserialized Object
     */
    <T> T deserialize(ByteBuffer buffer);

    /**
     * Deserializes given InputStream to an Object using Kryo instance in pool.
     * @param stream input stream
     * @param <T> deserialized Object type
     * @return deserialized Object
     */
    default <T> T deserialize(final InputStream stream) {
        return deserialize(stream, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Deserializes given InputStream to an Object using Kryo instance in pool.
     * @param stream input stream
     * @param <T> deserialized Object type
     * @param bufferSize size of the buffer in front of the stream
     * @return deserialized Object
     */
    <T> T deserialize(InputStream stream, int bufferSize);

}
