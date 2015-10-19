package org.unipop.elastic.controller.star2;

import com.fasterxml.jackson.databind.util.ArrayIterator;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.EdgeController;
import org.unipop.elastic.controller.edge.ElasticEdge;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseProperty;
import org.unipop.structure.UniGraph;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by sbarzilay on 10/18/15.
 */
public class NestedEdge extends BaseEdge {
    @Override
    public Iterator<Vertex> bothVertices() {
        return new ArrayIterator<>(new Vertex[]{inVertex(), outVertex()});
    }

    @Override
    public Vertex inVertex() {
        return inVertex;
    }

    @Override
    public Vertex outVertex() {
        return outVertex;
    }

    public NestedEdge(Object id, String label, Vertex inV, Vertex outV, UniGraph graph, EdgeController controller, Object... keyValues) {
        super(id, label, keyValues, outV, inV, controller, graph);
    }

    @Override
    protected void innerAddProperty(BaseProperty vertexProperty) {
        properties.put(vertexProperty.key(), vertexProperty);
    }

    @Override
    protected void innerRemoveProperty(Property property) {
        properties.remove(property.key());
    }

    @Override
    protected void innerRemove() {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        switch (direction) {
            case IN:
                return new ArrayIterator<>(new Vertex[]{inVertex()});
            case OUT:
                return new ArrayIterator<>(new Vertex[]{outVertex()});
            default:
                return bothVertices();
        }
    }

    public void fillFields(Map<String, Object> map) {
        map.forEach((key, value) -> {
            if (!key.equals(ElasticEdge.InId) ||
                    !key.equals(ElasticEdge.InLabel) ||
                    !key.equals(ElasticEdge.OutId) ||
                    !key.equals(ElasticEdge.OutLabel) ||
                    !key.equals("label"))
                properties.put(key, new BaseProperty<>(this, key, value));
        });
    }

    public Map<String, Object> getMap() {
        HashMap<String, Object> map = new HashMap<>();
        properties.forEach(map::put);
        map.put("label", label);
        map.put(ElasticEdge.InId, inVertex().id());
        map.put(ElasticEdge.InLabel, inVertex().label());
        map.put(ElasticEdge.OutId, outVertex().id());
        map.put(ElasticEdge.OutLabel, outVertex().label());
        return map;
    }
}
