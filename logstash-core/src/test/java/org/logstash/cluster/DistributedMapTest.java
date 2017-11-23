package org.logstash.cluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import org.apache.http.HttpHost;
import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public final class DistributedMapTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testGetSetFileBacked() throws Exception {
        try (
            DistributedClusterContext context = new DistributedClusterContext(
                UUID.randomUUID().toString(), new DfsBlockstore(temp.newFolder().getAbsolutePath())
            )
        ) {
            final DistributedMap set = new DistributedMap(context, "set");
            final String key = "foo";
            final String value = "bar";
            set.put(key, value);
            assertThat(set.get(key), is(value));
        }
    }

    @Test
    public void testGetSetEsBacked() throws Exception {
        try (
            DistributedClusterContext context = new DistributedClusterContext(
                UUID.randomUUID().toString(),
                new EsBlockstore(HttpHost.create("http://127.0.0.1:9200"))
            )
        ) {
            final DistributedMap set = new DistributedMap(context, UUID.randomUUID().toString());
            final String key = "foo";
            final String value = "bar";
            set.put(key, value);
            assertThat(set.get(key), is(value));
        }
    }

    @Test
    public void testKeyIteratorFileBacked() throws Exception {
        try (
            DistributedClusterContext context = new DistributedClusterContext(
                UUID.randomUUID().toString(), new DfsBlockstore(temp.newFolder().getAbsolutePath())
            )
        ) {
            final DistributedMap set = new DistributedMap(context, "set");
            final String value = "val";
            final Collection<String> keys = Arrays.asList("one", "two", "three");
            keys.forEach(keyy -> set.put(keyy, value));
            final Iterator<String> iterator = set.keyIterator();
            final Collection<String> found = new ArrayList<>();
            while (iterator.hasNext()) {
                found.add(iterator.next());
            }
            assertThat(found, CoreMatchers.hasItems(keys.toArray(new String[0])));
            assertThat(found.size(), is(keys.size()));
        }
    }

    @Test
    public void testKeyIteratorEsBacked() throws Exception {
        try (
            DistributedClusterContext context = new DistributedClusterContext(
                UUID.randomUUID().toString(),
                new EsBlockstore(HttpHost.create("http://127.0.0.1:9200"))
            )
        ) {
            final DistributedMap set = new DistributedMap(context, "set");
            final String value = "val";
            final Collection<String> keys = Arrays.asList("one", "two", "three");
            keys.forEach(keyy -> set.put(keyy, value));
            final Iterator<String> iterator = set.keyIterator();
            final Collection<String> found = new ArrayList<>();
            while (iterator.hasNext()) {
                found.add(iterator.next());
            }
            assertThat(found, CoreMatchers.hasItems(keys.toArray(new String[0])));
            assertThat(found.size(), is(keys.size()));
        }
    }
}
