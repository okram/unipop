package org.unipop.elastic.controller.star2;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.elastic.controller.edge.ElasticEdge;
import org.unipop.elastic.controller.star.StarController;
import org.unipop.elastic.controller.vertex.ElasticVertex;
import org.unipop.elastic.controller.vertex.ElasticVertexController;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.LazyGetter;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.UniGraph;

import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.lang.SuppressWarnings;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by sbarzilay on 10/18/15.
 */
public class StarVertex extends ElasticVertex {
    private Set<NestedEdge> nestedEdges;

    public StarVertex(Object id, String label, Object[] keyValues, UniGraph graph, LazyGetter lazyGetter, ElasticMutations elasticMutations, String indexName, ElasticVertexController controller) {
        super(id, label, keyValues, controller, graph, lazyGetter, elasticMutations, indexName);
        nestedEdges = new HashSet<>();
    }


    @Override
    public Iterator<Edge> cachedEdges(Direction direction, String[] edgeLabels, Predicates predicates) {
        ArrayList<Edge> edges = new ArrayList<>();
        if (direction.equals(Direction.IN)) return edges.iterator();
        nestedEdges.forEach(edge -> {
            if ((edgeLabels.length == 0 || StarController.contains(edgeLabels, edge.label()))) {
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

        return edges.iterator();
    }

    public void createEdges(Map<String, Object> source, EdgeController controller, String... propertyKeys) {
        for (String label : propertyKeys) {
            @SuppressWarnings("unchecked cast")
            ArrayList<Map<String, Object>> maps = ((ArrayList<Map<String, Object>>) source.get(label));
            if (maps != null) {
                for (Map<String, Object> map : maps) {
                    Predicates p = new Predicates();
                    p.hasContainers.add(new HasContainer(T.id.getAccessor(), P.eq(map.get(ElasticEdge.InId))));
                    p.hasContainers.add(new HasContainer(T.label.getAccessor(), P.eq(map.get(ElasticEdge.InLabel).toString())));
                    NestedEdge edge = new NestedEdge(map.get(ElasticEdge.OutId).toString() + map.get(ElasticEdge.InId).toString(),
                            label,
                            graph.getControllerManager().vertices(p, new MutableMetrics("vertexForEdge", "vertexForEdge")).next(),
                            this,
                            graph,
                            controller
                    );
                    edge.fillFields(source);
                    nestedEdges.add(edge);
                }
            }
        }
    }

    public BaseEdge addInnerEdge(Object edgeId, String label, Vertex inV, Object[] properties, EdgeController controller) {
        if (edgeId == null)
            edgeId = id.toString() + inV.id();
        NestedEdge edge = new NestedEdge(edgeId, label, inV, this, graph, controller, properties);
        nestedEdges.add(edge);
        return edge;
    }

    @Override
    public Map<String, Object> allFields() {
        Map<String, Object> map = super.allFields();
        Map<String, ArrayList<Map<String, Object>>> edges = new HashMap<>();
        nestedEdges.forEach(edge -> {
            if (edges.containsKey(edge.label())) {
                edges.get(edge.label()).add(edge.getMap());
            } else {
                edges.put(edge.label(), new ArrayList<Map<String, Object>>() {{
                    add(edge.getMap());
                }});
            }
        });
        edges.forEach(map::put);
        return map;
    }
}
