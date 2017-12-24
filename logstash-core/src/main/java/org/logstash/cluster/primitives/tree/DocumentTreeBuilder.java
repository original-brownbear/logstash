package org.logstash.cluster.primitives.tree;

import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitiveBuilder;
import org.logstash.cluster.primitives.Ordering;

/**
 * Builder for {@link DocumentTree}.
 */
public abstract class DocumentTreeBuilder<V>
    extends DistributedPrimitiveBuilder<DocumentTreeBuilder<V>, DocumentTree<V>, AsyncDocumentTree<V>> {

    private Ordering ordering = Ordering.NATURAL;

    public DocumentTreeBuilder() {
        super(DistributedPrimitive.Type.DOCUMENT_TREE);
    }

    /**
     * Sets the ordering of the tree nodes.
     * <p>
     * When {@link AsyncDocumentTree#getChildren(DocumentPath)} is called, children will be returned according to
     * the specified sort order.
     * @param ordering ordering of the tree nodes
     * @return this builder
     */
    public DocumentTreeBuilder<V> withOrdering(Ordering ordering) {
        this.ordering = ordering;
        return this;
    }

    /**
     * Returns the ordering of tree nodes.
     * <p>
     * When {@link AsyncDocumentTree#getChildren(DocumentPath)} is called, children will be returned according to
     * the specified sort order.
     * @return the ordering of tree nodes
     */
    public Ordering ordering() {
        return ordering;
    }

    @Override
    public DocumentTree<V> build() {
        return buildAsync().asDocumentTree();
    }
}
