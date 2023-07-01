package edu.uci.ics.texera.workflow.common.workflow.drove;

import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo;
import edu.uci.ics.texera.workflow.common.workflow.raven.CachedSubDAGs;

import java.util.*;
import java.util.stream.Collectors;

public class DrovePlusPlus extends Drove {
    Queue<Segment> segments = new PriorityQueue<>();
    CachedSubDAGs cachedWindows;
    public DrovePlusPlus(CachedSubDAGs cachedWindows, EV ev, WorkflowInfo.WorkflowDAG originalV1, WorkflowInfo.WorkflowDAG originalV2) {
        super(ev, originalV1, originalV2);
        this.cachedWindows = cachedWindows;
        initSegments();
    }

    private void initSegments() {
        long t0 = System.currentTimeMillis();
        // call init decomposition
        Decomposition initialDecomposition = initDecomposition();
        System.out.println("MAPPING " + (System.currentTimeMillis() - t0));
        // identify windows that are not supported
//        segments.add(new Segment(initialDecomposition));
        t0 = System.currentTimeMillis();
        Set<String> sinksNotEvaluated = new HashSet<>();
        Set<String> sinksEvaluated = new HashSet<>();
        List<Window> unsupportedWindows = initialDecomposition.windows.stream().filter(window -> !window.supported).collect(Collectors.toList());
        Queue<Decomposition> divided = initialDecomposition.divide(unsupportedWindows);
        divided.forEach(decomposition -> {
            if(decomposition.windows.stream().anyMatch(window -> window.covering)) {
              segments.add(new Segment(decomposition));
              sinksEvaluated.addAll(decomposition.getReachableSinks());
            }
            else {
                sinksNotEvaluated.addAll(decomposition.getReachableSinks());
            }
        });
        sinksNotEvaluated.removeAll(sinksEvaluated);
        unchangedSinks.addAll(sinksNotEvaluated);
        System.out.println("SEGMENTS " + (System.currentTimeMillis() - t0));
    }

    public Set<String> expandDecompositions(Decomposition seed) {
        List<Decomposition> explored= new ArrayList<>();
        Queue<Decomposition> decompositions = new PriorityQueue<>();
        decompositions.add(seed);
        Set<String> unchangedSinks = new HashSet<>();
        long t0 = System.currentTimeMillis();
        Decomposition decomposition;
        List<Window> windows;
        Window window;
        boolean isDecompositionMaximal;
        boolean isWindowMaximal;
        int countDecomp = 0;
        int countTested = 0;
        long evPerf = 0;
        Set<Decomposition> tempDecompositions = new HashSet<>();
        while(!decompositions.isEmpty()){
            decomposition = decompositions.poll();
            if(unchangedSinks.containsAll(decomposition.getReachableSinks())) {
                continue;
            }
            countDecomp++;
            isDecompositionMaximal = true;
            windows = decomposition.getWindows();
            for (Window value : windows) {
                tempDecompositions.clear();
                window = value;
                isWindowMaximal = true;
                if (window.covering && !window.maximal) {
                    Iterator<Window> neighbors = window.getNeighbors();
                    while (neighbors.hasNext()) {
                        Window neighbor = neighbors.next();
                        if (!neighbor.supported) {
                            continue;
                        }
                        Window merged = window.merge(neighbor, v1, v2);
                        if (ev.isValid(merged)) {
                            Decomposition newDecomposition = decomposition.createCloneMerge(window, neighbor, merged);
                            isDecompositionMaximal = false;
                            isWindowMaximal = false;
                            if (!explored.contains(newDecomposition)) {
                                tempDecompositions.add(newDecomposition);
                            }
                        }
                    }
                    if (isWindowMaximal && !isDecompositionMaximal) {
                        window.maximal = true;
                        // check for pruning only then add decompositions to queue
                        if (!window.tested) {
                            long tev = System.currentTimeMillis();
                            Set<String> result = window.getEquivalentSinks(true, cachedWindows, ev, v1, v2);
                            evPerf += System.currentTimeMillis() - tev;
                            countTested++;
                            if(!result.isEmpty()) {
//                            unchangedSinks.addAll(decomposition.getEquivalentSinks(ev, v1, v2));
//                            if (unchangedSinks.containsAll(allSinks)) {
//                                long perf = System.currentTimeMillis() - t0;
//                                System.out.println("AVG calling EV " + (evPerf / countTested));
//                                System.out.println("TOTAL calling EV " + evPerf);
//                                System.out.println("PERF of everything " + (perf - evPerf));
//                                System.out.println("NUMBER OF DECOMPOSITIONS EXPLORED " + countDecomp);
//                                System.out.println("NUMBER OF DECOMPOSITIONS TESTED " + countTested);
//                                return unchangedSinks; // early termination
//                            }
                                // only then add decompositions to be explored
                                decompositions.addAll(tempDecompositions);
                                explored.addAll(tempDecompositions);
                            }
                        }
                    } else {
                        // only then add decompositions to be explored
                        decompositions.addAll(tempDecompositions);
                        explored.addAll(tempDecompositions);
                    }
                }
            }
            if(isDecompositionMaximal){
                long tev = System.currentTimeMillis();
                unchangedSinks.addAll(decomposition.getEquivalentSinks(true, cachedWindows, ev, v1, v2));
                evPerf+= System.currentTimeMillis() - tev;
                countTested++;
                if(unchangedSinks.containsAll(allSinks)){
                    long perf = System.currentTimeMillis() - t0;
                    System.out.println("AVG calling EV " + (evPerf / countTested));
                    System.out.println("TOTAL calling EV " + evPerf);
                    System.out.println("PERF of everything " + (perf - evPerf));
                    System.out.println("NUMBER OF DECOMPOSITIONS EXPLORED " + countDecomp);
                    System.out.println("NUMBER OF DECOMPOSITIONS TESTED " + countTested);
                    return unchangedSinks;
                }
            }
        }
        long perf = System.currentTimeMillis() - t0;
        System.out.println("AVG calling EV " + (evPerf / countTested));
        System.out.println("TOTAL calling EV " + evPerf);
        System.out.println("PERF of everything " + (perf - evPerf));
        System.out.println("NUMBER OF DECOMPOSITIONS EXPLORED " + countDecomp);
        System.out.println("NUMBER OF DECOMPOSITIONS TESTED " + countTested);
        return unchangedSinks;
    }

    @Override
    public Set<String> getEquivalentSinks() {
        long t0 = System.currentTimeMillis();
//        if(allSinks.stream().allMatch(sink -> isInequivalent(sink))) {
//            long inequiv = System.currentTimeMillis() - t0;
//            System.out.println("inequivalence " + inequiv);
//            System.out.println("=====================================");
//            return unchangedSinks;
//        }
        if (segments != null) {
            while(!segments.isEmpty()) {
                Segment segment = segments.poll();
                System.out.println("segment ");
                Set<String> decompositionUnchangedSinks = expandDecompositions(segment.initialDecomposition);
                if(decompositionUnchangedSinks.isEmpty()) {
                    return new HashSet<>();
                }
                unchangedSinks.removeIf(sink -> segment.initialDecomposition.getReachableSinks().contains(sink) && !decompositionUnchangedSinks.contains(sink));
                unchangedSinks.addAll(decompositionUnchangedSinks);
            }
        }
        unchangedSinks.forEach(sink -> System.out.println("UNCHANGED " + sink));
        System.out.println("============================");
        return unchangedSinks;
    }
}
