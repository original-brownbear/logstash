package org.logstash.cluster.storage.buffer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Direct memory bit set test.
 */
public class BitArrayTest {

    /**
     * Tests the bit array.
     */
    @Test
    public void testBitArray() {
        BitArray bits = BitArray.allocate(1024);

        for (int i = 0; i < 1024; i++) {
            assertFalse(bits.get(i));
        }

        for (int i = 0; i < 64; i++) {
            bits.set(i);
        }

        for (int i = 64; i < 1024; i++) {
            assertFalse(bits.get(i));
        }

        for (int i = 0; i < 1024; i++) {
            bits.set(i);
        }

        assertEquals(bits.count(), 1024);

        for (int i = 0; i < 1024; i++) {
            assertTrue(bits.get(i));
        }
    }

}
