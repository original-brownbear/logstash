package org.logstash.cluster.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Array size hash printer test.
 */
public class ArraySizeHashPrinterTest {
    @Test
    public void testArraySizeHashPrinter() {
        ArraySizeHashPrinter printer = ArraySizeHashPrinter.of(new byte[]{1, 2, 3});
        assertEquals("byte[]{length=3, hash=30817}", printer.toString());
    }
}
