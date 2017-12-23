package org.logstash.cluster.storage.buffer;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Abstract bytes implementation.
 * <p>
 * This class provides common state and bounds checking functionality for all {@link Bytes} implementations.
 */
public abstract class AbstractBytes implements Bytes {
    private boolean open = true;
    private SwappedBytes swap;

    /**
     * Checks bounds for a read.
     */
    protected int checkRead(int offset, int length) {
        checkOffset(offset);
        int position = offset + length;
        if (position > size())
            throw new BufferUnderflowException();
        return position;
    }

    /**
     * Checks that the offset is within the bounds of the buffer.
     */
    protected void checkOffset(int offset) {
        checkOpen();
        if (offset < 0 || offset > size())
            throw new IndexOutOfBoundsException();
    }

    /**
     * Checks whether the block is open.
     */
    protected void checkOpen() {
        if (!open)
            throw new IllegalStateException("bytes not open");
    }

    /**
     * Checks bounds for a write.
     */
    protected int checkWrite(int offset, int length) {
        checkOffset(offset);
        int position = offset + length;
        if (position > size())
            throw new BufferOverflowException();
        return position;
    }

    @Override
    public int readUnsignedByte(int offset) {
        return readByte(offset) & 0xFF;
    }

    @Override
    public boolean isDirect() {
        return false;
    }

    @Override
    public int readUnsignedShort(int offset) {
        return readShort(offset) & 0xFFFF;
    }

    @Override
    public boolean isFile() {
        return false;
    }

    @Override
    public int readMedium(int offset) {
        return (readByte(offset)) << 16
            | (readByte(offset + 1) & 0xff) << 8
            | (readByte(offset + 2) & 0xff);
    }

    @Override
    public ByteOrder order() {
        return ByteOrder.BIG_ENDIAN;
    }

    @Override
    public int readUnsignedMedium(int offset) {
        return (readByte(offset) & 0xff) << 16
            | (readByte(offset + 1) & 0xff) << 8
            | (readByte(offset + 2) & 0xff);
    }

    @Override
    public Bytes order(ByteOrder order) {
        if (order == null)
            throw new NullPointerException("order cannot be null");
        if (order == order())
            return this;
        if (swap != null)
            return swap;
        swap = new SwappedBytes(this);
        return swap;
    }

    @Override
    public long readUnsignedInt(int offset) {
        return readInt(offset) & 0xFFFFFFFFL;
    }

    @Override
    public boolean readBoolean(int offset) {
        return readByte(offset) == 1;
    }

    @Override
    public String readString(int offset) {
        return readString(offset, Charset.defaultCharset());
    }

    @Override
    public String readString(int offset, Charset charset) {
        if (readBoolean(offset)) {
            byte[] bytes = new byte[readUnsignedShort(offset + BYTE)];
            read(offset + BYTE + SHORT, bytes, 0, bytes.length);
            return new String(bytes, charset);
        }
        return null;
    }

    @Override
    public String readUTF8(int offset) {
        return readString(offset, StandardCharsets.UTF_8);
    }

    @Override
    public Bytes writeUnsignedByte(int offset, int b) {
        return writeByte(offset, (byte) b);
    }

    @Override
    public Bytes writeUnsignedShort(int offset, int s) {
        return writeShort(offset, (short) s);
    }

    @Override
    public Bytes writeMedium(int offset, int m) {
        writeByte(offset, (byte) (m >>> 16));
        writeByte(offset + 1, (byte) (m >>> 8));
        writeByte(offset + 2, (byte) m);
        return this;
    }

    @Override
    public Bytes writeUnsignedMedium(int offset, int m) {
        return writeMedium(offset, m);
    }

    @Override
    public Bytes writeUnsignedInt(int offset, long i) {
        return writeInt(offset, (int) i);
    }

    @Override
    public Bytes writeBoolean(int offset, boolean b) {
        return writeByte(offset, b ? 1 : 0);
    }

    @Override
    public Bytes writeString(int offset, String s) {
        return writeString(offset, s, Charset.defaultCharset());
    }

    @Override
    public Bytes writeString(int offset, String s, Charset charset) {
        if (s == null) {
            return writeBoolean(offset, Boolean.FALSE);
        } else {
            writeBoolean(offset, Boolean.TRUE);
            byte[] bytes = s.getBytes(charset);
            return writeUnsignedShort(offset + BYTE, bytes.length)
                .write(offset + BYTE + SHORT, bytes, 0, bytes.length);
        }
    }

    @Override
    public Bytes writeUTF8(int offset, String s) {
        return writeString(offset, s, StandardCharsets.UTF_8);
    }

    @Override
    public Bytes flush() {
        return this;
    }

    @Override
    public void close() {
        open = false;
    }

}
