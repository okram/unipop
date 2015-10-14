package org.unipop.elastic.basic.java;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.unipop.elastic.basic.BasicGraphProvider;
import org.unipop.elastic.basic.StarGraphProvider;
import org.unipop.structure.UniGraph;
import org.junit.runner.RunWith;

@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = StarGraphProvider.class, graph = UniGraph.class)
public class UniGraphStructureStandardTest {
}
