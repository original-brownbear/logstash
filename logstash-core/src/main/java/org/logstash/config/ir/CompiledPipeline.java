package org.logstash.config.ir;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.jruby.RubyArray;
import org.jruby.RubyHash;
import org.jruby.runtime.builtin.IRubyObject;
import org.logstash.RubyUtil;
import org.logstash.Rubyfier;
import org.logstash.common.SourceWithMetadata;
import org.logstash.config.ir.compiler.Dataset;
import org.logstash.config.ir.compiler.EventCondition;
import org.logstash.config.ir.compiler.RubyIntegration;
import org.logstash.config.ir.graph.IfVertex;
import org.logstash.config.ir.graph.PluginVertex;
import org.logstash.config.ir.graph.Vertex;
import org.logstash.config.ir.imperative.PluginStatement;
import org.logstash.ext.JrubyEventExtLibrary;

/**
 * <h3>Compiled Logstash Pipeline Configuration.</h3>
 * This class represents an executable pipeline, compiled from the configured topology that is
 * learnt from {@link PipelineIR}.
 * Each compiled pipeline consists in graph of {@link Dataset} that represent either a {@code filter}
 * or an {@code if} condition.
 */
public final class CompiledPipeline {

    private final Collection<IRubyObject> inputs;

    private final HashMap<String, RubyIntegration.Filter> filters = new HashMap<>();

    private final Collection<RubyIntegration.Output> outputs = new HashSet<>();

    private final PipelineIR graph;

    private final RubyIntegration.Pipeline pipeline;

    private final ThreadLocal<Dataset> cachedDataset = new ThreadLocal<>();

    public CompiledPipeline(final PipelineIR graph, final RubyIntegration.Pipeline pipeline) {
        this.graph = graph;
        this.pipeline = pipeline;
        inputs = setupInputs();
        setupFilters();
        setupOutputs();
    }

    public RubyIntegration.Plugin registerPlugin(final RubyIntegration.Plugin plugin) {
        plugin.register();
        return plugin;
    }

    public void filter(final JrubyEventExtLibrary.RubyEvent event, final RubyArray generated) {
        final RubyArray incoming = RubyUtil.RUBY.newArray();
        incoming.add(event);
        Dataset dataset = cachedDataset.get();
        if (dataset == null) {
            dataset = buildDataset();
            cachedDataset.set(dataset);
        }
        generated.addAll(dataset.compute(incoming));
        dataset.clear();
    }

    public void output(final RubyArray events) {
        outputs.forEach(output -> output.multiReceive(events));
    }

    public Collection<RubyIntegration.Filter> shutdownFlushers() {
        return filters.values().stream().filter(RubyIntegration.Filter::hasFlush).collect(
            Collectors.toList());
    }

    public Collection<RubyIntegration.Filter> periodicFlushers() {
        return shutdownFlushers().stream().filter(
            filter -> filter.periodicFlush()).collect(Collectors.toList());
    }

    public Collection<RubyIntegration.Output> outputs() {
        return outputs;
    }

    public Collection<RubyIntegration.Filter> filters() {
        return new ArrayList<>(filters.values());
    }

    public Collection<IRubyObject> inputs() {
        return inputs;
    }

    /**
     * Sets up all Ruby outputs learnt from {@link PipelineIR}.
     */
    private void setupOutputs() {
        graph.getOutputPluginVertices().forEach(v -> {
            final PluginDefinition def = v.getPluginDefinition();
            final SourceWithMetadata source = v.getSourceWithMetadata();
            outputs.add(pipeline.buildOutput(
                RubyUtil.RUBY.newString(def.getName()), RubyUtil.RUBY.newFixnum(source.getLine()),
                RubyUtil.RUBY.newFixnum(source.getColumn()), convertArgs(def)
            ));
        });
    }

    /**
     * Sets up all Ruby filters learnt from {@link PipelineIR}.
     */
    private void setupFilters() {
        for (final PluginVertex plugin : graph.getFilterPluginVertices()) {
            final String ident = plugin.getId();
            if (!filters.containsKey(ident)) {
                filters.put(ident, buildFilter(plugin));
            }
        }
    }

    private Collection<IRubyObject> setupInputs() {
        final Collection<PluginVertex> vertices = graph.getInputPluginVertices();
        final Collection<IRubyObject> nodes = new HashSet<>(vertices.size());
        vertices.forEach(v -> {
            final PluginDefinition def = v.getPluginDefinition();
            final SourceWithMetadata source = v.getSourceWithMetadata();
            nodes.add(pipeline.buildInput(
                RubyUtil.RUBY.newString(def.getName()), RubyUtil.RUBY.newFixnum(source.getLine()),
                RubyUtil.RUBY.newFixnum(source.getColumn()), convertArgs(def)
            ));
        });
        return nodes;
    }

    /**
     * Converts plugin arguments from the format provided by {@link PipelineIR} into coercible
     * Ruby types.
     * @param def PluginDefinition as provided by {@link PipelineIR}
     * @return RubyHash of plugin arguments as understood by {@link RubyIntegration.Pipeline}
     * methods
     */
    private RubyHash convertArgs(final PluginDefinition def) {
        final RubyHash converted = RubyHash.newHash(RubyUtil.RUBY);
        for (final Map.Entry<String, Object> entry : def.getArguments().entrySet()) {
            final Object value = entry.getValue();
            final String key = entry.getKey();
            final Object toput;
            if (value instanceof PluginStatement) {
                final PluginDefinition codec = ((PluginStatement) value).getPluginDefinition();
                toput = pipeline.buildCodec(
                    RubyUtil.RUBY.newString(codec.getName()),
                    Rubyfier.deep(RubyUtil.RUBY, codec.getArguments())
                );
            } else {
                toput = value;
            }
            converted.put(key, toput);
        }
        return converted;
    }

    private RubyIntegration.Filter buildFilter(final PluginVertex vertex) {
        final PluginDefinition def = vertex.getPluginDefinition();
        final SourceWithMetadata source = vertex.getSourceWithMetadata();
        return pipeline.buildFilter(
            RubyUtil.RUBY.newString(def.getName()), RubyUtil.RUBY.newFixnum(source.getLine()),
            RubyUtil.RUBY.newFixnum(source.getColumn()),
            Rubyfier.deep(RubyUtil.RUBY, convertArgs(def))
        );
    }

    private Dataset buildDataset() {
        final Map<String, Dataset> filterplugins = new HashMap<>(this.filters.size());
        final Collection<Dataset> datasets = new ArrayList<>(5);
        // We sort the leaves of the graph in a deterministic fashion before compilation.
        // This is not strictly necessary for correctness since it will only influence the order
        // of output events for which Logstash makes no guarantees, but it greatly simplifies
        // testing and is no issue performance wise since compilation only happens on pipeline
        // reload.
        graph.getGraph().getAllLeaves().stream().sorted(Comparator.comparing(Vertex::hashPrefix))
            .forEachOrdered(
                leaf -> {
                    final Collection<Dataset> parents =
                        flatten(Dataset.ROOT_DATASETS, leaf, filterplugins);
                    if (isFilter(leaf)) {
                        datasets.add(filterDataset(leaf.getId(), filterplugins, parents));
                    } else if (leaf instanceof IfVertex) {
                        datasets.add(splitRight(parents, buildCondition((IfVertex) leaf)));
                    } else {
                        datasets.addAll(parents);
                    }
                }
            );
        return new Dataset.SumDataset(datasets);
    }

    private boolean isFilter(final Vertex vertex) {
        return graph.getFilterPluginVertices().contains(vertex);
    }

    /**
     * Compiles the next level of the execution from the given {@link Vertex}.
     * @param parents Nodes from the last already compiled level
     * @param start Vertex to compile children for
     * @param cached Cache of already compiled {@link Dataset}
     * @return Datasets originating from given {@link Vertex}
     */
    private Collection<Dataset> flatten(final Collection<Dataset> parents, final Vertex start,
        final Map<String, Dataset> cached) {
        final Collection<Vertex> endings = start.getIncomingVertices();
        if (endings.isEmpty()) {
            return parents;
        }
        final Collection<Dataset> res = new ArrayList<>(2);
        for (final Vertex end : endings) {
            Collection<Dataset> newparents = flatten(parents, end, cached);
            if (newparents.isEmpty()) {
                newparents = new ArrayList<>(parents);
            }
            if (isFilter(end)) {
                res.add(filterDataset(end.getId(), cached, newparents));
            } else if (end instanceof IfVertex) {
                final IfVertex ifvert = (IfVertex) end;
                final EventCondition iff = buildCondition(ifvert);
                if (ifvert.getOutgoingBooleanEdgesByType(true).stream()
                    .anyMatch(edge -> Objects.equals(edge.getTo(), start))) {
                    res.add(splitLeft(newparents, iff));
                } else {
                    res.add(splitRight(newparents, iff));
                }
            }
        }
        return res;
    }

    private Dataset filterDataset(final String vertex, final Map<String, Dataset> cache,
        final Collection<Dataset> parents) {
        final Dataset dataset;
        if (cache.containsKey(vertex)) {
            dataset = cache.get(vertex);
        } else {
            dataset = new Dataset.FilteredDataset(parents, filters.get(vertex));
            cache.put(vertex, dataset);
        }
        return dataset;
    }

    private static Dataset splitRight(final Collection<Dataset> parents,
        final EventCondition condition) {
        return splitLeft(parents, EventCondition.Compiler.not(condition));
    }

    private static Dataset splitLeft(final Collection<Dataset> parents,
        final EventCondition condition) {
        return new Dataset.SplitDataset(parents, condition);
    }

    /**
     * Compiles an {@link IfVertex} into an {@link EventCondition}.
     * @param iff IfVertex to build condition for
     * @return EventCondition for given {@link IfVertex}
     */
    private static EventCondition buildCondition(final IfVertex iff) {
        return EventCondition.Compiler.buildCondition(iff.getBooleanExpression());
    }

}
