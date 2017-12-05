package org.logstash.cluster.io;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

public final class InetSocketAddressSerializer implements Serializer<InetSocketAddress> {

    public static final InetSocketAddressSerializer INSTANCE = new InetSocketAddressSerializer();

    private InetSocketAddressSerializer() {
        // Singleton
    }

    @Override
    public void serialize(@NotNull final DataOutput2 out,
        @NotNull final InetSocketAddress value) throws IOException {
        final byte[] raw = value.getAddress().getAddress();
        out.writeInt(raw.length);
        out.write(raw);
        out.writeInt(value.getPort());
    }

    @Override
    public InetSocketAddress deserialize(@NotNull final DataInput2 input,
        final int available) throws IOException {
        final byte[] raw = new byte[input.readInt()];
        input.readFully(raw);
        return new InetSocketAddress(InetAddress.getByAddress(raw), input.readInt());
    }
}
