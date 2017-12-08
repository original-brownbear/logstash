package org.logstash.cluster.storage.buffer;

import org.logstash.cluster.utils.memory.NativeMemory;

/**
 * Native byte buffer implementation.
 */
public abstract class NativeBuffer extends AbstractBuffer {

    protected NativeBuffer(NativeBytes bytes, int offset, int initialCapacity, int maxCapacity) {
        super(bytes, offset, initialCapacity, maxCapacity, null);
    }

    @Override
    protected void compact(int from, int to, int length) {
        NativeMemory memory = ((NativeBytes) bytes).memory;
        NativeMemory.unsafe().copyMemory(memory.address(from), memory.address(to), length);
        NativeMemory.unsafe().setMemory(memory.address(from), length, (byte) 0);
    }

}
