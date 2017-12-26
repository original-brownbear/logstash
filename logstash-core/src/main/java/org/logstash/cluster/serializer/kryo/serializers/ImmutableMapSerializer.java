package org.logstash.cluster.serializer.kryo.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/**
 * Kryo Serializer for {@link ImmutableMap}.
 */
public class ImmutableMapSerializer extends Serializer<ImmutableMap<?, ?>> {

    /**
     * Creates {@link ImmutableMap} serializer instance.
     */
    public ImmutableMapSerializer() {
        // non-null, immutable
        super(false, true);
    }

    @Override
    public void write(Kryo kryo, Output output, ImmutableMap<?, ?> object) {
        output.writeInt(object.size());
        for (Map.Entry<?, ?> e : object.entrySet()) {
            kryo.writeClassAndObject(output, e.getKey());
            kryo.writeClassAndObject(output, e.getValue());
        }
    }

    @Override
    public ImmutableMap<?, ?> read(Kryo kryo, Input input,
        Class<ImmutableMap<?, ?>> type) {
        final int size = input.readInt();
        switch (size) {
            case 0:
                return ImmutableMap.of();
            case 1:
                return ImmutableMap.of(kryo.readClassAndObject(input),
                    kryo.readClassAndObject(input));

            default:
                ImmutableMap.Builder<Object, Object> builder = ImmutableMap.builder();
                for (int i = 0; i < size; ++i) {
                    builder.put(kryo.readClassAndObject(input),
                        kryo.readClassAndObject(input));
                }
                return builder.build();
        }
    }
}
