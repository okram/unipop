package org.unipop.process;

import org.unipop.controller.Predicates;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.StandardTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.controllerprovider.ControllerManager;

import java.util.Iterator;
import java.util.Optional;

public class UniGraphStartStep<E extends Element> extends GraphStep<E> {

    private final Predicates predicates;
    private final ControllerManager controllerManager;
    private MutableMetrics metrics;

    public UniGraphStartStep(GraphStep originalStep, Predicates predicates, ControllerManager controllerManager) {
        super(originalStep.getTraversal(), originalStep.getReturnClass(), originalStep.getIds());
        originalStep.getLabels().forEach(label -> this.addLabel(label.toString()));
        predicates.labels.forEach(label -> this.addLabel(label.toString()));
        this.predicates = predicates;
        this.controllerManager = controllerManager;
        Optional<StandardTraversalMetrics> metrics = this.getTraversal().asAdmin().getSideEffects().<StandardTraversalMetrics>get(TraversalMetrics.METRICS_KEY);
        if(metrics.isPresent()) this.metrics = (MutableMetrics) metrics.get().getMetrics(this.getId());
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
    }

    private Iterator<? extends Vertex> vertices() {
        return controllerManager.vertices(predicates, metrics);
    }

    private Iterator<? extends Edge> edges() {
         return controllerManager.edges(predicates, metrics);
    }
}
