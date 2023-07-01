package edu.uci.ics.texera.workflow.common.workflow.drove;

import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo;
import edu.uci.ics.texera.workflow.common.workflow.raven.CachedSubDAGs;

import java.util.*;

public class Decomposition implements Comparable<Decomposition>{

    List<Window> windows;

    public Decomposition() {
        windows = new ArrayList<>();
    }
    public Decomposition(List<Window> windows) {
        this.windows = windows;
    }

    public Decomposition createCloneMerge(Window first, Window second, Window merged){
        HashMap<Window, Window> clonedMapping = new HashMap<>();
        Decomposition newDecomposition = new Decomposition();
        newDecomposition.windows.add(merged);
        clonedMapping.put(first, merged);
        clonedMapping.put(second, merged);
        deepCloneMerge(first, second, clonedMapping, newDecomposition.windows);
        return newDecomposition;
    }

    public Queue<Decomposition> divide(List<Window> unsupportedWindows) {
        Queue<Decomposition> dividedDecompositions = new PriorityQueue<>();
        Set<Window> visitedWindows = new HashSet<>();
        unsupportedWindows.forEach(window -> {
            if(!visitedWindows.containsAll(window.upstreams)) {
                Decomposition newDecomposition1 = new Decomposition();
                deepCloneDivide(window, new HashMap<>(), newDecomposition1.windows, unsupportedWindows, true);
                dividedDecompositions.add(newDecomposition1);
                visitedWindows.addAll(newDecomposition1.windows);
                visitedWindows.add(window);
            }
            if(!visitedWindows.containsAll(window.downstreams)) {
                Decomposition newDecomposition2 = new Decomposition();
                deepCloneDivide(window, new HashMap<>(), newDecomposition2.windows, unsupportedWindows, false);
                dividedDecompositions.add(newDecomposition2);
                visitedWindows.addAll(newDecomposition2.windows);
                visitedWindows.add(window);
            }
        });
        if(dividedDecompositions.isEmpty() && unsupportedWindows.isEmpty()) {
            dividedDecompositions.add(this);
        }
        return dividedDecompositions;
    }

    public Set<String> getEquivalentSinks(boolean checkCacheFlag, CachedSubDAGs cachedSubDAGs, EV ev, WorkflowInfo.WorkflowDAG originalv1, WorkflowInfo.WorkflowDAG originalv2) {
        Set<String> unchangedSinks = new HashSet<>();
        for(int i = 0; i < windows.size(); i++) {
            Window window = windows.get(i);
            if(!window.covering) {
                continue;
            }
            Set<String> windowUnchangedSinks = window.getEquivalentSinks(checkCacheFlag, cachedSubDAGs, ev, originalv1, originalv2);
            if(windowUnchangedSinks.isEmpty()) {
                return new HashSet<>();
            }
            unchangedSinks.removeIf(sink -> window.getReachableSinks().contains(sink) && !windowUnchangedSinks.contains(sink));
            unchangedSinks.addAll(windowUnchangedSinks);
        }
    return unchangedSinks;
    }

    public Set<String> getReachableSinks() {
        Set<String> reachableSinks = new HashSet<>();
        windows.forEach(window -> reachableSinks.addAll(window.reachableSinks));
        return reachableSinks;
    }

    public List<Window> getWindows() {
        return windows;
    }

    private void deepCloneMerge(Window first, Window second, HashMap<Window, Window> clonedMap, List<Window> windows) {
        Queue<Window> q = new LinkedList<>();
        q.add(first);
        q.add(second);
        while (!q.isEmpty()) {
            Window w = q.poll();
            Window cloned =clonedMap.get(w);
            for(Window n : w.upstreams) {
                Window clonedN = buildEdges(n, clonedMap, q, windows);
                // first check if not the merged node
                if(!((w == first || w == second) && (n == first || n == second))) {
                    cloned.upstreams.add(clonedN);
                }
            }
            for(Window n : w.downstreams) {
                Window clonedN = buildEdges(n, clonedMap, q, windows);
                // first check if not the merged node
                if(!((w == first || w == second) && (n == first || n == second))) {
                    cloned.downstreams.add(clonedN);
                }
            }
        }
    }

    private void deepCloneDivide(Window unsupported, HashMap<Window, Window> clonedMap, List<Window> newWindows, List<Window> unsupportedWindows, boolean upstreamFlag) {
        Queue<Window> q = new LinkedList<>();
        if(upstreamFlag){
            unsupported.upstreams.forEach(upstream -> {
                Window cloned = upstream.deepCopy();
                newWindows.add(cloned);
                clonedMap.put(upstream, cloned);
                q.add(upstream);
            });
        }
        else {
            unsupported.downstreams.forEach(downstream -> {
                Window cloned = downstream.deepCopy();
                newWindows.add(cloned);
                clonedMap.put(downstream, cloned);
                q.add(downstream);
            });
        }
        while (!q.isEmpty()) {
            Window w = q.poll();
            Window cloned =clonedMap.get(w);
            for(Window n : w.upstreams) {
                // first check if not any unsupported windows
                if(!unsupportedWindows.contains(n)) {
                    Window clonedN = buildEdges(n, clonedMap, q, newWindows);
                    cloned.upstreams.add(clonedN);
                }
            }
            for(Window n : w.downstreams) {
                // first check if not the merged node
                if(!unsupportedWindows.contains(n)) {
                    Window clonedN = buildEdges(n, clonedMap, q, newWindows);
                    cloned.downstreams.add(clonedN);
                }
            }
        }
    }

    private Window buildEdges(Window n, HashMap<Window, Window> clonedMap, Queue<Window> q, List<Window> windows) {
        Window clonedN = clonedMap.get(n);
        if(clonedN == null) {
            q.add(n);
            clonedN = n.deepCopy();
            clonedMap.put(n,clonedN);
            windows.add(clonedN);
        }
        return clonedN;
    }

    private int getNumberOfWindows() {
        return windows.size();
    }

    private int largestSizeCoveringWindow() {
        final int[] largest = {0};
        windows.forEach(window -> {if(window.v1.operators.size() > largest[0]) {
        largest[0] = window.v1.operators.size();
        }
        });
        return largest[0] * -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Decomposition that = (Decomposition) o;
        return windows.containsAll(that.windows) && that.windows.containsAll(windows);
    }

    @Override
    public int hashCode() {
        return Objects.hash(windows);
    }

    @Override
    public int compareTo(Decomposition o) {
        //TODO unmerged windows (negative) and number of operators in window (positive)
        return Comparator.comparing(Decomposition::getNumberOfWindows).thenComparing(Decomposition::largestSizeCoveringWindow).compare(this, o);
    }
}
