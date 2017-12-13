package org.logstash.cluster.serializer.kryo.serializers;

import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;

/**
 * Default serializers.
 */
public class DefaultSerializers {

    /**
     * Basic serializer.
     */
    public static final Serializer BASIC = Serializer.using(KryoNamespaces.BASIC);

    private DefaultSerializers() {
    }
}
