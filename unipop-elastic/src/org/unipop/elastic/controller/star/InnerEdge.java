package org.unipop.elastic.controller.star;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.unipop.structure.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.Iterator;

public class InnerEdge extends BaseEdge {

    private final EdgeMapping mapping;
    private Vertex outVertex, inVertex;

    public InnerEdge(Object edgeId, EdgeMapping mapping, Vertex outVertex, Vertex inVertex, Object[] keyValues, UniGraph graph) {
        super(edgeId, mapping.getLabel(), keyValues, graph);
        this.mapping = mapping;
        this.outVertex = outVertex;
        this.inVertex = inVertex;
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    protected void innerRemoveProperty(Property property) {
        throw new NotImplementedException();
    }

    @Override
    protected void innerRemove() {
        throw new NotImplementedException();
    }

    @Override
    protected void innerAddProperty(BaseProperty vertexProperty) {
        properties.put(vertexProperty.key(), vertexProperty);
    }

    public EdgeMapping getMapping() {
        return mapping;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        ArrayList<Vertex> vertices = new ArrayList<>();
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            vertices.add(inVertex);
        }
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            vertices.add(outVertex);
        }
        return vertices.iterator();
    }
}
