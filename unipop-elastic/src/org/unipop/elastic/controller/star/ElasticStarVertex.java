package org.unipop.elastic.controller.star;

import org.unipop.controller.Predicates;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.LazyGetter;
import org.unipop.elastic.controller.vertex.ElasticVertex;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.structure.*;

import java.util.*;

public class ElasticStarVertex extends ElasticVertex {
    private Set<InnerEdge> innerEdges;

    public ElasticStarVertex(final Object id,
                             final String label,
                             Object[] keyValues,
                             UniGraph graph,
                             LazyGetter lazyGetter,
                             ElasticMutations elasticMutations,
                             String indexName) {
        super(id, label, keyValues, graph, lazyGetter, elasticMutations, indexName);
        innerEdges = new HashSet<>();
    }

    public void createEdges(List<EdgeMapping> mappings,
                            Map<String,Object>source){
        mappings.forEach(edgeMapping -> addEdges(edgeMapping, source));
    }

    private void addEdges(EdgeMapping edgeMapping, Map<String,Object>source) {
        Iterable<Object> vertices = edgeMapping.getExternalVertexId(source);
        vertices.forEach(externalId -> {
            InnerEdge edge = new InnerEdge(id.toString() + externalId.toString(),
                    edgeMapping,
                    this,
                    graph.getControllerProvider().vertex(null,null,externalId, edgeMapping.getExternalVertexLabel()),
                    edgeMapping.getProperties(source, externalId),
                    graph);
            innerEdges.add(edge);
        });
    }


    @Override
    public Iterator<Edge> cachedEdges(Direction direction, String[] edgeLabels, Predicates predicates) {
        ArrayList<Edge> edges = new ArrayList<>();
        innerEdges.forEach(edge -> {
            EdgeMapping mapping = edge.getMapping();
            if (mapping.getDirection().equals(direction) &&
                    (edgeLabels.length == 0 || StarController.contains(edgeLabels, mapping.getLabel()))) {

                // Test predicates on inner edge
                boolean passed = true;
                for (HasContainer hasContainer : predicates.hasContainers) {
                    if (!hasContainer.test(edge)) {
                        passed = false;
                    }
                }
                if (passed) {
                    edges.add(edge);
                }
            }
        });

        if (edges.size() > 0) return edges.iterator();
        else return null;
    }

    public Edge addInnerEdge(EdgeMapping mapping, Object edgeId, Vertex inV, Object[] properties) {
        InnerEdge edge = new InnerEdge(edgeId,mapping,this,inV,properties,graph);
        innerEdges.add(edge);
        return edge;
    }
}
