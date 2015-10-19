package org.unipop.elastic.controller.star2;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.NestedFilterBuilder;
import org.elasticsearch.search.SearchHit;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.elastic.controller.edge.ElasticEdge;
import org.unipop.elastic.controller.vertex.ElasticVertex;
import org.unipop.elastic.controller.vertex.ElasticVertexController;
import org.unipop.elastic.helpers.*;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.LazyGetter;
import org.unipop.elastic.helpers.QueryIterator;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.lang.InterruptedException;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

/**
 * Created by sbarzilay on 10/18/15.
 */
public class Star2Controller extends ElasticVertexController implements EdgeController {
    private String[] edgeLabels;

    public Star2Controller(UniGraph graph, Client client, ElasticMutations elasticMutations, String defaultIndex, int scrollSize, boolean refresh, TimingAccessor timing, String... edgeLabels) {
        super(graph, client, elasticMutations, defaultIndex, scrollSize, refresh, timing);
        this.edgeLabels = edgeLabels;
    }

    @Override
    public Iterator<BaseEdge> edges(Object[] ids) {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates, MutableMetrics metrics) {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<BaseEdge> fromVertex(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics) {
        ArrayList<BaseEdge> edges = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            for (Vertex vertex : vertices) {
                ((StarVertex) vertex).cachedEdges(Direction.OUT, edgeLabels, predicates).forEachRemaining(edge -> edges.add(((NestedEdge) edge)));
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            BoolFilterBuilder bool = FilterBuilders.boolFilter();
            Object[] ids = new Object[vertices.length];
            for (int i = 0; i < vertices.length; i++) {
                ids[i] = vertices[i].id();
            }
            if (edgeLabels.length > 0) {
                for (String label : edgeLabels) {
                    NestedFilterBuilder filter = FilterBuilders.nestedFilter(label, ElasticHelper.createFilterBuilder(predicates.hasContainers));
                    bool.should(filter);
                }
            } else {
                for (String label : this.edgeLabels) {
                    Predicates p = new Predicates();
                    p.hasContainers.add(new HasContainer(ElasticEdge.InId, P.within(ids)));
                    NestedFilterBuilder filter = FilterBuilders.nestedFilter(label, ElasticHelper.createFilterBuilder(p.hasContainers));
                    bool.should(filter);
                }
            }
            QueryIterator<Vertex> edgesVertices =
                    new QueryIterator<>(bool,
                            (int) predicates.limitLow,
                            scrollSize, predicates.limitHigh,
                            client,
                            this::createVertex,
                            refresh,
                            timing,
                            getDefaultIndex());
            edgesVertices.forEachRemaining(vertex -> ((StarVertex) vertex).cachedEdges(Direction.OUT, edgeLabels, predicates).forEachRemaining(edge -> edges.add(((NestedEdge) edge))));
        }
        return edges.iterator();
    }

    @Override
    protected ElasticVertex createVertex(Object id, String label, Object[] keyValues, LazyGetter lazyGetter) {
        StarVertex vertex = new StarVertex(id, label, keyValues, graph, lazyGetter, elasticMutations, getDefaultIndex(), this);
        return vertex;
    }

    public BaseVertex vertex(Edge edge, Direction direction, Object vertexId, String vertexLabel) {
        Predicates p = new Predicates();
        p.hasContainers.add(new HasContainer(T.id.getAccessor(), P.eq(vertexId)));
        p.hasContainers.add(new HasContainer(T.label.getAccessor(), P.eq(vertexLabel)));
        return vertices(p, null).next();
    }

    @Override
    protected BaseVertex createVertex(SearchHit hit) {
        StarVertex vertex = new StarVertex(hit.id(), hit.type(), null, graph, new LazyGetter(client, timing, refresh), elasticMutations, getDefaultIndex(), this);
        hit.getSource().entrySet().forEach((field) -> vertex.addPropertyLocal(field.getKey(), field.getValue()));
        vertex.createEdges(hit.getSource(), this, edgeLabels);
        return vertex;
    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        BaseEdge edge = ((StarVertex) outV).addInnerEdge(edgeId, label, inV, properties, this);
        try {
            elasticMutations.updateElement(outV, getDefaultIndex(), null, false);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        return edge;
    }
}
