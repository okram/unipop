package org.unipop.elastic.controller.star;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.search.SearchHit;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.elastic.controller.edge.ElasticEdge;
import org.unipop.elastic.controller.vertex.ElasticVertex;
import org.unipop.elastic.controller.vertex.ElasticVertexController;
import org.unipop.elastic.helpers.*;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;
import java.util.concurrent.ExecutionException;


public class StarController extends ElasticVertexController implements EdgeController {

    private ArrayList<EdgeMapping> edgeMappings;


    public StarController(UniGraph graph, Client client, ElasticMutations elasticMutations, String defaultIndex,
                          int scrollSize, boolean refresh, TimingAccessor timing, EdgeMapping... edgeMappings) {
        super(graph, client, elasticMutations, defaultIndex, scrollSize, timing);
        this.edgeMappings = new ArrayList<>();
        Collections.addAll(this.edgeMappings, edgeMappings);
    }

    @Override
    protected BaseVertex createVertex(SearchHit hit) {
        ElasticStarVertex vertex = new ElasticStarVertex(hit.id(), hit.type(), null, graph, new LazyGetter(client, timing), this, elasticMutations, getDefaultIndex());
        hit.getSource().entrySet().forEach((field) -> vertex.addPropertyLocal(field.getKey(), field.getValue()));
        vertex.createEdges(edgeMappings, hit.getSource());
        return vertex;
    }

    @Override
    protected ElasticVertex createVertex(Object id, String label, Map<String, Object> keyValues, LazyGetter lazyGetter) {
        return new ElasticStarVertex(id,label,keyValues,graph,lazyGetter,this,elasticMutations,getDefaultIndex());
    }

    @Override
    public BaseVertex fromEdge(Direction direction, Object vertexId, String vertexLabel) {
        Predicates predicates = new Predicates();
        predicates.hasContainers.add(new HasContainer("~label", P.eq(vertexLabel)));
        predicates.hasContainers.add(new HasContainer("~id", P.eq(vertexId)));
        return vertices(predicates,new MutableMetrics("fromEdge","fromEdge")).next();
    }



    @Override
    public Iterator<BaseEdge> edges(Object[] ids) {
        throw new NotImplementedException();
    }

    private Object getId(Predicates p) {
        for (HasContainer has : p.hasContainers) {
            if (has.getKey().equals("~id"))
                return has.getValue();
        }
        return null;
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates, MutableMetrics metrics) {
        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        OrFilterBuilder mappingFilter = FilterBuilders.orFilter();
        boolean empty = true;
        for (EdgeMapping mapping : edgeMappings) {
            Object id = getId(predicates);
            if (id != null) {
                mappingFilter.add(FilterBuilders.termsFilter(mapping.getExternalVertexField(), id));
            }
            empty = false;
        }
        if (!empty) {
            boolFilter.must(mappingFilter);
        }
        QueryIterator<BaseVertex> results = new QueryIterator<>(boolFilter, 0, scrollSize,
                predicates.limitHigh - predicates.limitLow, client, this::createVertex, timing, getDefaultIndex());
        ArrayList<BaseEdge> edges = new ArrayList<>();
        String[] labels = predicates.labels.toArray(new String[0]);
        results.forEachRemaining(vertex -> vertex.cachedEdges(Direction.BOTH, labels, predicates).forEachRemaining(edge -> edges.add(((BaseEdge) edge))));
        return edges.iterator();
    }

    @Override
    public Iterator<BaseEdge> fromVertex(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics) {
        String[] vertexIds = new String[vertices.length];
        for (int i = 0; i < vertices.length; i++) vertexIds[i] = vertices[i].id().toString();

        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        OrFilterBuilder mappingFilter = FilterBuilders.orFilter();
        boolean empty = true;
        for (EdgeMapping mapping : edgeMappings) {
            if (edgeLabels != null && edgeLabels.length > 0 && !contains(edgeLabels, mapping.getLabel())) continue;
            mappingFilter.add(FilterBuilders.idsFilter().addIds(vertexIds));
            empty = false;
        }
        if (!empty) {
            boolFilter.must(mappingFilter);
        }

        QueryIterator<BaseVertex> results = new QueryIterator<>(boolFilter, 0, scrollSize,
                predicates.limitHigh - predicates.limitLow, client, this::createVertex, timing, getDefaultIndex());

        ArrayList<BaseEdge> edges = new ArrayList<>();
        results.forEachRemaining(vertex -> vertex.cachedEdges(direction, edgeLabels, predicates).forEachRemaining(edge -> edges.add(((BaseEdge) edge))));
        return edges.iterator();
    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Map<String, Object> properties) {
        EdgeMapping mapping = getEdgeMapping(label, Direction.OUT);
        if (mapping == null) {
            mapping = new NestedEdgeMapping(label, inV.label(), Direction.OUT, label);
            edgeMappings.add(mapping);
        }
        BaseEdge edge = ((ElasticStarVertex) outV).addInnerEdge(mapping, edgeId, inV, properties);
        try {
            elasticMutations.updateElement(outV, getDefaultIndex(), null, true);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        return edge;
    }

    public static boolean contains(String[] edgeLabels, String label) {
        for (String edgeLabel : edgeLabels)
            if (edgeLabel.equals(label)) return true;
        return false;
    }

    private EdgeMapping getEdgeMapping(String label, Direction direction) {
        for (EdgeMapping mapping : edgeMappings) {
            if (mapping.getLabel().equals(label) && mapping.getDirection().equals(direction)) {
                return mapping;
            }
        }
        return null;
    }

    @Override
    public Iterator<BaseVertex> vertices(Object[] vertexIds) {
        Predicates predicates = new Predicates();
        predicates.hasContainers.add(new HasContainer("~id",P.within(vertexIds)));
        return vertices(predicates,new MutableMetrics("vertexById","vertexById"));
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates, MutableMetrics metrics) {
        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        boolFilter.must(FilterBuilders.missingFilter(ElasticEdge.InId));
        return new QueryIterator<>(boolFilter, 0, scrollSize, predicates.limitHigh - predicates.limitLow,
                client, this::createVertex, timing, getDefaultIndex());
    }
}
