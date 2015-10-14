package org.unipop.elastic.controller.star;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.controller.Predicates;

import java.util.Iterator;
import java.util.Map;

public interface EdgeMapping {

    String getExternalVertexField();

    Direction getDirection();

    Object[] getProperties(Map<String, Object> entries, Object id);

    String getLabel() ;

    String getExternalVertexLabel() ;

    Iterable<Object> getExternalVertexId(Map<String, Object> entries);
}
