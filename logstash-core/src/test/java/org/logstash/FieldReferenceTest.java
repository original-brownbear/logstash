package org.logstash;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class FieldReferenceTest {

    @Test
    public void testParseSingleBareField() throws Exception {
        assertArrayEquals(PathCache.parse("foo"), new String[]{"foo"});
    }

    @Test
    public void testParseSingleFieldPath() throws Exception {
        assertArrayEquals(PathCache.parse("[foo]"), new String[]{"foo"});
    }

    @Test
    public void testParse2FieldsPath() throws Exception {
        assertArrayEquals(PathCache.parse("[foo][bar]"), new String[]{"foo", "bar"});
    }

    @Test
    public void testParse3FieldsPath() throws Exception {
        assertArrayEquals(
            PathCache.parse("[foo][bar][baz]"), new String[]{"foo", "bar", "baz"}
        );
    }
}
