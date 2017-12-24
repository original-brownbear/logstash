package org.logstash.cluster.serializer.kryo;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.logstash.cluster.serializer.kryo.serializers.ArraysAsListSerializer;
import org.logstash.cluster.serializer.kryo.serializers.ImmutableListSerializer;
import org.logstash.cluster.serializer.kryo.serializers.ImmutableMapSerializer;
import org.logstash.cluster.serializer.kryo.serializers.ImmutableSetSerializer;

public final class KryoNamespaces {

    public static final int BASIC_MAX_SIZE = 50;
    public static final KryoNamespace BASIC = KryoNamespace.builder()
        .nextId(KryoNamespace.FLOATING_ID)
        .register(byte[].class)
        .register(AtomicBoolean.class)
        .register(AtomicInteger.class)
        .register(AtomicLong.class)
        .register(new ImmutableListSerializer(),
            ImmutableList.class,
            ImmutableList.of(1).getClass(),
            ImmutableList.of(1, 2).getClass(),
            ImmutableList.of(1, 2, 3).subList(1, 3).getClass())
        .register(new ImmutableSetSerializer(),
            ImmutableSet.class,
            ImmutableSet.of().getClass(),
            ImmutableSet.of(1).getClass(),
            ImmutableSet.of(1, 2).getClass())
        .register(new ImmutableMapSerializer(),
            ImmutableMap.class,
            ImmutableMap.of().getClass(),
            ImmutableMap.of("a", 1).getClass(),
            ImmutableMap.of("R", 2, "D", 2).getClass())
        .register(Collections.unmodifiableSet(Collections.emptySet()).getClass())
        .register(HashMap.class)
        .register(ConcurrentHashMap.class)
        .register(CopyOnWriteArraySet.class)
        .register(ArrayList.class,
            LinkedList.class,
            HashSet.class,
            LinkedHashSet.class
        )
        .register(HashMultiset.class)
        .register(Sets.class)
        .register(Maps.immutableEntry("a", "b").getClass())
        .register(new ArraysAsListSerializer(), Arrays.asList().getClass())
        .register(Collections.singletonList(1).getClass())
        .register(Duration.class)
        .register(Collections.emptySet().getClass())
        .register(Optional.class)
        .register(Collections.emptyList().getClass())
        .register(Collections.singleton(Object.class).getClass())
        .register(int[].class)
        .register(long[].class)
        .register(short[].class)
        .register(double[].class)
        .register(float[].class)
        .register(char[].class)
        .register(String[].class)
        .register(boolean[].class)
        .build("BASIC");

    /**
     * Kryo registration Id for user custom registration.
     */
    public static final int BEGIN_USER_CUSTOM_ID = 500;

    // not to be instantiated
    private KryoNamespaces() {
    }
}
