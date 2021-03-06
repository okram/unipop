package org.unipop.elastic.controller.edge;

import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.QueryIterator;
import org.unipop.elastic.helpers.TimingAccessor;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.unipop.controller.*;
import org.unipop.structure.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;

import java.util.*;

public class ElasticEdgeController implements org.unipop.controller.EdgeController {
    private UniGraph graph;
    private final Client client;
    private final ElasticMutations elasticMutations;
    private final String indexName;
    private final int scrollSize;
    private TimingAccessor timing;

    public ElasticEdgeController(UniGraph graph, Client client, ElasticMutations elasticMutations, String indexName,
                                 int scrollSize, TimingAccessor timing) {
        this.graph = graph;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.indexName = indexName;
        this.scrollSize = scrollSize;
        this.timing = timing;
    }

    @Override
    public Iterator<BaseEdge> edges(Object[] ids) {
        MultiGetRequest request = new MultiGetRequest();
        for (Object id : ids) request.add(indexName, null, id.toString());
        MultiGetResponse responses = client.multiGet(request).actionGet();

        ArrayList<BaseEdge> elements = new ArrayList<>(ids.length);
        for (MultiGetItemResponse getResponse : responses) {
            GetResponse response = getResponse.getResponse();
            if (!response.isExists()) throw Graph.Exceptions.elementNotFound(Edge.class, response.getId());
            elements.add(createEdge(response.getId(), response.getType(), response.getSource()));
        }
        return elements.iterator();
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates, MutableMetrics metrics) {
        elasticMutations.refresh();
        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        boolFilter.must(FilterBuilders.existsFilter(ElasticEdge.InId));
        return new QueryIterator<>(boolFilter, 0, scrollSize, predicates.limitHigh - predicates.limitLow,
                client, this::createEdge, timing, indexName);
    }

    @Override
    public Iterator<BaseEdge> fromVertex(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics) {
        elasticMutations.refresh();

        Object[] vertexIds = new Object[vertices.length];
        for(int i = 0; i < vertices.length; i++) vertexIds[i] = vertices[i].id();

        if (edgeLabels != null && edgeLabels.length > 0)
            predicates.hasContainers.add(new HasContainer(T.label.getAccessor(), P.within(edgeLabels)));

        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        if (direction == Direction.IN)
            boolFilter.must(FilterBuilders.termsFilter(ElasticEdge.InId, vertexIds));
        else if (direction == Direction.OUT)
            boolFilter.must(FilterBuilders.termsFilter(ElasticEdge.OutId, vertexIds));
        else if (direction == Direction.BOTH)
            boolFilter.must(FilterBuilders.orFilter(
                    FilterBuilders.termsFilter(ElasticEdge.InId, vertexIds),
                    FilterBuilders.termsFilter(ElasticEdge.OutId, vertexIds)));

        return new QueryIterator<>(boolFilter, 0, scrollSize, predicates.limitHigh - predicates.limitLow, client, this::createEdge, timing, indexName);
    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Map<String, Object> properties) {
        ElasticEdge elasticEdge = new ElasticEdge(edgeId, label, properties, outV, inV,this, graph, elasticMutations, indexName);
        try {
            elasticMutations.addElement(elasticEdge, indexName, null, true);
        }
        catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.edgeWithIdAlreadyExists(elasticEdge.id());
        }
        return elasticEdge;
    }

    private BaseEdge createEdge(SearchHit hit) {
        return createEdge(hit.id(), hit.getType(), hit.getSource());
    }

    private BaseEdge createEdge(String id, String label, Map<String, Object> fields) {
        BaseVertex outV = this.graph.getControllerManager().fromEdge(Direction.OUT, fields.get(ElasticEdge.OutId), fields.get(ElasticEdge.OutLabel).toString());
        BaseVertex inV = this.graph.getControllerManager().fromEdge(Direction.IN, fields.get(ElasticEdge.InId), fields.get(ElasticEdge.InLabel).toString());
        BaseEdge edge = new ElasticEdge(id, label, fields, outV, inV, this,  graph, elasticMutations, indexName);
        return edge;
    }
}
