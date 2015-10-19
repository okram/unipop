package org.unipop.elastic.controller.star2;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.client.Client;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.controllerprovider.ControllerManager;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.util.Iterator;

/**
 * Created by sbarzilay on 10/19/15.
 */
public class StarManager implements ControllerManager {
    private UniGraph graph;
    private Client client;
    private VertexController controller;
    private EdgeController edgeController;
    private TimingAccessor timing;

    @Override
    public void init(UniGraph graph, Configuration configuration) throws IOException {
        this.graph = graph;
        timing = new TimingAccessor();
        client = ElasticClientFactory.create(configuration);
        Star2Controller star = new Star2Controller(graph, client, new ElasticMutations(false, client, timing), "graph", 10, false, timing, "edge");
        controller = star;
        edgeController = star;
    }

    @Override
    public void commit() {
        graph.commit();
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
    public Iterator<BaseEdge> edges(Object[] ids) {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates, MutableMetrics metrics) {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<BaseEdge> fromVertex(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics) {
        return edgeController.fromVertex(vertices, direction, edgeLabels, predicates, metrics);
    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        return edgeController.addEdge(edgeId, label, outV, inV, properties);
    }

    @Override
    public Iterator<BaseVertex> vertices(Object[] ids) {
        return controller.vertices(ids);
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates, MutableMetrics metrics) {
        return controller.vertices(predicates, metrics);
    }

    @Override
    public BaseVertex fromEdge(Direction direction, Object vertexId, String vertexLabel) {
        return null;
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Object[] properties) {
        return controller.addVertex(id, label, properties);
    }
}
