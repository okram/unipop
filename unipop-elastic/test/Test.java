import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic.basic.StarControllerProvider;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.structure.UniGraph;

/**
 * Created by sbarzilay on 10/16/15.
 */
public class Test {
    private UniGraph graph;

    public Test() throws InstantiationException {
        BaseConfiguration conf= new BaseConfiguration();
        conf.addProperty("elasticsearch.client", ElasticClientFactory.ClientType.TRANSPORT_CLIENT);
        conf.addProperty("elasticsearch.refresh", "true");
        conf.addProperty("controllerProvider", StarControllerProvider.class.getCanonicalName());
        graph = new UniGraph(conf);
    }

    @org.junit.Test
    public void test(){
        Vertex v = graph.addVertex("test");
        Vertex v2 = graph.addVertex("test2");
        v.addEdge("knows", v2);
        GraphTraversal t = graph.traversal().V().hasLabel("test").out();
        procecessTravrsal(t);
    }

    private void procecessTravrsal(GraphTraversal t){
        while (t.hasNext()){
            System.out.println(t.next());
        }
    }
}
