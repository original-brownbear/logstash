package org.logstash.cluster.primitives.tree.impl;

import org.logstash.cluster.primitives.DistributedPrimitiveCreator;
import org.logstash.cluster.primitives.DistributedPrimitives;
import org.logstash.cluster.primitives.tree.AsyncDocumentTree;
import org.logstash.cluster.primitives.tree.DocumentTreeBuilder;

/**
 * Default {@link AsyncDocumentTree} builder.
 * @param <V> type for document tree value
 */
public class DefaultDocumentTreeBuilder<V> extends DocumentTreeBuilder<V> {

    private final DistributedPrimitiveCreator primitiveCreator;

    public DefaultDocumentTreeBuilder(DistributedPrimitiveCreator primitiveCreator) {
        this.primitiveCreator = primitiveCreator;
    }

    @Override
    public AsyncDocumentTree<V> buildAsync() {
        AsyncDocumentTree<V> tree = primitiveCreator.newAsyncDocumentTree(name(), serializer(), ordering());
        tree = relaxedReadConsistency() ? DistributedPrimitives.newCachingDocumentTree(tree) : tree;
        return tree;
    }
}
