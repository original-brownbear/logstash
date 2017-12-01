package org.logstash.cluster;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

interface LogEntry extends Writable {

    long getTerm();

    long getIndex();

    void apply(ClusterState state);

    final class DeleteLogEntry implements LogEntry {

        private long term;

        private long index;

        private byte[] prefix;

        DeleteLogEntry(final long term, final long index, final byte[] prefix) {
            this.term = term;
            this.index = index;
            this.prefix = prefix;
        }

        @Override
        public long getTerm() {
            return term;
        }

        @Override
        public long getIndex() {
            return index;
        }

        @Override
        public void apply(final ClusterState state) {
            state.delete(prefix);
        }

        @Override
        public void readNext(final DataInput input) throws IOException {
            term = input.readLong();
            index = input.readLong();
            prefix = new byte[input.readInt()];
            input.readFully(prefix);
        }

        @Override
        public void writeTo(final DataOutput output) throws IOException {
            output.writeLong(term);
            output.writeLong(index);
            output.writeInt(prefix.length);
            output.write(prefix);
        }
    }

    final class PutLogEntry implements LogEntry {

        private long term;

        private long index;

        private byte[] key;

        private byte[] value;

        PutLogEntry(final long term, final long index, final byte[] key, final byte[] value) {
            this.term = term;
            this.index = index;
            this.key = key.clone();
            this.value = value.clone();
        }

        @Override
        public long getTerm() {
            return term;
        }

        @Override
        public long getIndex() {
            return index;
        }

        @Override
        public void apply(final ClusterState state) {
            final int len = value.length;
            final byte[] data = new byte[len + 4];
            data[0] = (byte) (len >>> 24 & 0xFF);
            data[1] = (byte) (len >>> 16 & 0xFF);
            data[2] = (byte) (len >>> 8 & 0xFF);
            data[3] = (byte) (len & 0xFF);
            state.put(key, new DataInputStream(new ByteArrayInputStream(data)));
        }

        @Override
        public void readNext(final DataInput input) throws IOException {
            term = input.readLong();
            index = input.readLong();
            key = new byte[input.readInt()];
            input.readFully(key);
            value = new byte[input.readInt()];
            input.readFully(value);
        }

        @Override
        public void writeTo(final DataOutput output) throws IOException {
            output.writeLong(term);
            output.writeLong(index);
            output.writeInt(key.length);
            output.write(key);
            output.writeInt(value.length);
            output.write(value);
        }
    }
}
