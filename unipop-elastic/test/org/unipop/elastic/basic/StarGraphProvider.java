package org.unipop.elastic.basic;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.unipop.elastic.ElasticGraphProvider;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by sbarzilay on 10/14/15.
 */
public class StarGraphProvider extends ElasticGraphProvider {
    public StarGraphProvider() throws IOException, ExecutionException, InterruptedException {
    }
    @Override
    public Configuration newGraphConfiguration(String graphName, Class<?> test, String testMethodName, Map<String, Object> configurationOverrides, LoadGraphWith.GraphData loadGraphWith) {
        Configuration configuration = super.newGraphConfiguration(graphName, test, testMethodName, configurationOverrides, loadGraphWith);
        configuration.setProperty("controllerProvider", StarControllerProvider.class.getName());
        configuration.setProperty("elasticsearch.refresh", "true");
        return configuration;
    }
}
