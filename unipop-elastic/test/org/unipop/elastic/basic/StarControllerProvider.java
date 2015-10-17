package org.unipop.elastic.basic;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.client.Client;
import org.unipop.controller.Predicates;
import org.unipop.controller.virtualvertex.VirtualVertexController;
import org.unipop.controllerprovider.ControllerProvider;
import org.unipop.elastic.controller.star.BasicEdgeMapping;
import org.unipop.elastic.controller.star.StarController;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by sbarzilay on 10/14/15.
 */
public class StarControllerProvider implements ControllerProvider{
    private StarController controller;
    private Client client;
    private ElasticMutations elasticMutations;
    private TimingAccessor timing;

    @Override
    public void init(UniGraph graph, Configuration configuration) throws IOException {
        String indexName = configuration.getString("elasticsearch.index.name", "graph");
        boolean refresh = configuration.getBoolean("elasticsearch.refresh", true);
        int scrollSize = configuration.getInt("elasticsearch.scrollSize", 500);
        boolean bulk = configuration.getBoolean("elasticsearch.bulk", false);

        client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, client);

        timing = new TimingAccessor();
        elasticMutations = new ElasticMutations(bulk, client, timing);

        controller = new StarController(graph,
                client,
                elasticMutations,
                indexName,
                scrollSize,
                refresh,
                timing,
                new BasicEdgeMapping("knows","vertex",Direction.OUT,"edges","prop")
                );
    }

    @Override
    public void commit() {
        elasticMutations.commit();
    }

    @Override
    public void printStats() {
        timing.print();
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public Iterator<Edge> edges(Object[] ids) {
        return controller.edges(ids);
    }

    @Override
    public Iterator<Edge> edges(Predicates predicates, MutableMetrics metrics) {
        return controller.edges(predicates,metrics);
    }

    @Override
    public Iterator<Edge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics) {
        return controller.edges(vertices,direction,edgeLabels,predicates,metrics);
    }

    @Override
    public Edge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        return controller.addEdge(edgeId,label,outV,inV,properties);
    }

    @Override
    public Iterator<Vertex> vertices(Object[] ids) {
        return controller.vertices(ids);
    }

    @Override
    public Iterator<Vertex> vertices(Predicates predicates, MutableMetrics metrics) {
        return controller.vertices(predicates,metrics);
    }

    @Override
    public BaseVertex vertex(Edge edge, Direction direction, Object vertexId, String vertexLabel) {
        return controller.vertex(edge,direction,vertexId,vertexLabel);
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Object[] properties) {
        return controller.addVertex(id,label,properties);
    }
}
