package edu.uci.ics.texera.workflow.common.workflow.drove;

import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo;
import edu.uci.ics.texera.workflow.common.workflow.raven.CachedSubDAGs;

import java.util.*;

public class Window {

    boolean covering = false;
    boolean maximal = false;
    boolean supported = true;
    boolean tested = false;
    Set<Window> upstreams = new HashSet<>();
    Set<Window> downstreams = new HashSet<>();
    WorkflowDag v1;
    WorkflowDag v2;
    Set<String> reachableSinks;

    public Window(WorkflowDag v1, WorkflowDag v2, Set<String> reachableSinks) {
    this.v1 = v1;
    this.v2 = v2;
    this.reachableSinks = reachableSinks;
    }

    public Iterator<Window> getNeighbors() {
        Set<Window> neighbors = new HashSet<>(upstreams);
        neighbors.addAll(downstreams);
        return neighbors.iterator();
    }

    public Window merge(Window other, WorkflowInfo.WorkflowDAG originalv1, WorkflowInfo.WorkflowDAG originalv2) {
        Window merged;
        if(downstreams.contains(other)) {
            merged = other.deepCopy();
            merged.mergeLeft(this, originalv1, originalv2);
        }
        else {
            merged  = this.deepCopy();
            merged.mergeLeft(other, originalv1, originalv2);
        }
        if(other.covering || covering){
            merged.covering = true;
        }
        return merged;
    }

    private void mergeLeft(Window window, WorkflowInfo.WorkflowDAG originalv1, WorkflowInfo.WorkflowDAG originalv2) {
        v1.mergeLeft(window.v1, originalv1);
        v2.mergeLeft(window.v2, originalv2);
        reachableSinks.addAll(window.reachableSinks);
    }

    public Set<String> getEquivalentSinks(boolean checkCacheFlag, CachedSubDAGs cachedSubDAGs, EV ev, WorkflowInfo.WorkflowDAG originalv1, WorkflowInfo.WorkflowDAG originalv2) {
        Window cloned = deepCopy();
        cloned.addVirtualSourcesAndSinks(originalv1, originalv2);
        tested = true;
//        if(checkCacheFlag && cachedSubDAGs != null) {
//            return cachedSubDAGs.getEquivalentSinks(ev, cloned.v1, cloned.v2);
//        }
        return ev.getEquivalentSinks(cloned.v1, cloned.v2);
    }

    public void addVirtualSourcesAndSinks(WorkflowInfo.WorkflowDAG originalv1, WorkflowInfo.WorkflowDAG originalv2) {
        v1.addVirtualSourcesAndSinks(originalv1);
        v2.addVirtualSourcesAndSinks(originalv2);
    }

    public Set<String> getReachableSinks() {
        return reachableSinks;
    }

    protected Window deepCopy() {
        Window copy = new Window(v1.deepCopy(), v2.deepCopy(), new HashSet<>(reachableSinks));
        copy.maximal = maximal;
        copy.covering = covering;
        copy.supported = supported;
        copy.tested = tested;
        return copy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Window window = (Window) o;
        return Objects.equals(v1, window.v1) && Objects.equals(v2, window.v2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(v1, v2);
    }
}
