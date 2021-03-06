package org.unipop.elastic.misc;

import org.apache.tinkerpop.gremlin.*;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.*;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.unipop.elastic.ElasticGraphProvider;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

public class TemporaryTests extends AbstractGremlinTest {

    public TemporaryTests() throws InterruptedException, ExecutionException, IOException {
        GraphManager.setGraphProvider(new ElasticGraphProvider());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_Drop() throws Exception {
        GraphTraversal traversal =  g.V().and(has("age", P.gt(27)), outE().count().is(P.gte(2l))).values("name");
        check(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void get_g_VX1X_out_hasIdX2X() {
        GraphTraversal traversal =   g.V().barrier().coalesce(out());

//        traversal =   g.V().branch().
//                option(TraversalOptionParent.Pick.any,  values("age")).
//                option(TraversalOptionParent.Pick.any, values("name"));

        check(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeat() {
        GraphTraversal<Vertex, Vertex> traversal = g.V().repeat(out()).times(1);
        //GraphTraversal traversal = g.V().out();
        check(traversal);
    }

    private void check(GraphTraversal traversal) {
        System.out.println("pre-strategy:" + traversal);
        traversal.hasNext();
        System.out.println("post-strategy:" + traversal);

        //traversal.profile().cap(TraversalMetrics.METRICS_KEY);

        while(traversal.hasNext())
            System.out.println(traversal.next());
    }
}
