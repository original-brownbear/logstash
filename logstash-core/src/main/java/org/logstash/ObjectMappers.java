package org.logstash;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.NonTypedScalarSerializerBase;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import java.io.IOException;
import org.jruby.RubyString;

public final class ObjectMappers {

    public static final ObjectMapper JSON_MAPPER = new ObjectMapper()
        .registerModule(new AfterburnerModule());

    public static final ObjectMapper CBOR_MAPPER = new ObjectMapper(
        new CBORFactory().configure(CBORGenerator.Feature.WRITE_MINIMAL_INTS, false)
    ).registerModule(new AfterburnerModule());

    static {
        final SimpleModule stringModule = new SimpleModule("SimpleModule");
        stringModule.addSerializer(RubyString.class, new RubyStringSerializer());
        JSON_MAPPER.registerModule(stringModule);
        CBOR_MAPPER.registerModule(stringModule);
    }

    private ObjectMappers() {
    }

    private static final class RubyStringSerializer
        extends NonTypedScalarSerializerBase<RubyString> {

        RubyStringSerializer() {
            super(RubyString.class, true);
        }

        @Override
        public void serialize(final RubyString value, final JsonGenerator generator,
            final SerializerProvider provider)
            throws IOException {
            generator.writeString(value.asJavaString());
        }

    }
}
