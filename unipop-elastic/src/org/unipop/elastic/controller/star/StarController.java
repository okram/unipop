package org.unipop.elastic.controller.star;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.search.SearchHit;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.elastic.controller.edge.ElasticEdge;
import org.unipop.elastic.controller.vertex.ElasticVertex;
import org.unipop.elastic.controller.vertex.ElasticVertexController;
import org.unipop.elastic.helpers.*;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class StarController extends ElasticVertexController implements EdgeController {

    private EdgeMapping[] edgeMappings;

    public StarController(UniGraph graph, Client client, ElasticMutations elasticMutations, String defaultIndex,
                          int scrollSize, boolean refresh, TimingAccessor timing, EdgeMapping... edgeMappings) {
        super(graph, client, elasticMutations, defaultIndex, scrollSize, refresh, timing);
        this.edgeMappings = edgeMappings;
    }

    @Override
    protected BaseVertex createVertex(SearchHit hit) {
        ElasticStarVertex vertex = new ElasticStarVertex(hit.id(),hit.type(),null,graph,new LazyGetter(client,timing,refresh),elasticMutations,getDefaultIndex());
        hit.getSource().entrySet().forEach((field) -> vertex.addPropertyLocal(field.getKey(), field.getValue()));
        vertex.createEdges(Arrays.asList(edgeMappings), hit.getSource());
        return vertex;
    }

    @Override
    public Iterator<Edge> edges(Object[] ids) {
        return null;
    }

    @Override
    public Iterator<Edge> edges(Predicates predicates, MutableMetrics metrics) {
        ArrayList<Edge> edgeArrayList = new ArrayList<>();
        vertices(predicates,metrics).forEachRemaining(vertex -> {
                    ElasticStarVertex star = (ElasticStarVertex) vertex;
                    star.cachedEdges(Direction.OUT,new String[0],predicates).forEachRemaining(edgeArrayList::add);
                });
        return edgeArrayList.iterator();
    }

    @Override
    public Iterator<Edge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics) {
        Object[] vertexIds = new Object[vertices.length];
        for(int i = 0; i < vertices.length; i++) vertexIds[i] = vertices[i];

        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        OrFilterBuilder mappingFilter = FilterBuilders.orFilter();
        boolean empty = true;
        for (EdgeMapping mapping : edgeMappings) {
            if (edgeLabels != null && edgeLabels.length > 0 && !contains(edgeLabels, mapping.getLabel())) continue;
            mappingFilter.add(FilterBuilders.termsFilter(mapping.getExternalVertexField(), vertexIds));
            empty = false;
        }
        if (!empty) {
            boolFilter.must(mappingFilter);
        }

        QueryIterator<BaseVertex> results = new QueryIterator<>(boolFilter, 0, scrollSize,
                predicates.limitHigh - predicates.limitLow, client, this::createVertex, refresh, timing, getDefaultIndex());

        return new Iterator<Edge>() {
            public Iterator<Edge> currentIterator = EmptyIterator.instance();

            @Override
            public boolean hasNext() {
                return currentIterator.hasNext() || results.hasNext();
            }

            @Override
            public Edge next() {
                if(!currentIterator.hasNext())
                    currentIterator = results.next().cachedEdges(direction, edgeLabels, predicates);
                return currentIterator.next();
            }
        };
    }

    public static boolean contains(String[] edgeLabels, String label) {
        for (String edgeLabel : edgeLabels)
            if (edgeLabel.equals(label)) return true;
        return false;
    }

    @Override
    public Edge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {

        EdgeMapping mapping = getEdgeMapping(label, Direction.OUT );
        if(mapping != null)
            return ((ElasticStarVertex)outV).addInnerEdge(mapping, edgeId, inV, properties);
        return null;
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
    public BaseVertex vertex(Edge edge, Direction direction, Object vertexId, String vertexLabel) {
        Predicates p = new Predicates();
        p.hasContainers.add(new HasContainer("~id", P.eq(vertexId)));
        p.hasContainers.add(new HasContainer("~label", P.eq(vertexLabel)));
        return (BaseVertex) vertices(p,new MutableMetrics("","")).next();
    }
}
