package edu.uci.ics.texera.workflow.common.workflow.drove;

import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo;
import java.util.*;

public class DroveBaseline extends Drove{
    List<Decomposition> explored= new ArrayList<>();

    public DroveBaseline(EV ev, WorkflowInfo.WorkflowDAG v1, WorkflowInfo.WorkflowDAG v2) {
        super(ev, v1, v2);
    }

    @Override
    public void exploreDecompositions(Decomposition seed) {
        Queue<Decomposition> decompositions = new LinkedList<>();
        decompositions.add(seed);
        long t0 = System.currentTimeMillis();
        Decomposition decomposition;
        List<Window> windows;
        Window window;
        boolean isDecompositionMaximal;
        boolean isWindowMaximal;
        int countDecomp = 0;
        int countTested = 0;
        long evPerf = 0;
        int maximalIndex = 0;
        int numberofOpsinWindow = 0;
        while(!decompositions.isEmpty()){
            decomposition = decompositions.poll();
            if(unchangedSinks.containsAll(decomposition.getReachableSinks())) {
                continue;
            }
            countDecomp++;
            isDecompositionMaximal = true;
            windows = decomposition.getWindows();
            for(int i = 0; i < windows.size(); i++){
                window = windows.get(i);
                isWindowMaximal = true;
                if(window.covering && !window.maximal){
                    Iterator<Window> neighbors = window.getNeighbors();
                    while (neighbors.hasNext()) {
                        Window neighbor = neighbors.next();
                        if(!neighbor.supported) {
                            continue;
                        }
                        Window merged = window.merge(neighbor, v1, v2);
                        if(ev.isValid(merged))
                        {
                            Decomposition newDecomposition = decomposition.createCloneMerge(window, neighbor, merged);
                        isDecompositionMaximal = false;
                        isWindowMaximal = false;
                        if(!explored.contains(newDecomposition)) {
                            decompositions.add(newDecomposition);
                                explored.add(newDecomposition);
                        }
                        }
                    }
                    if(isWindowMaximal){
                        maximalIndex = i;
                        window.maximal = true;
                    }
                }
            }
            if(isDecompositionMaximal){
                long tev = System.currentTimeMillis();
                unchangedSinks.addAll(decomposition.getEquivalentSinks(false, null, ev, v1, v2));
                evPerf+= System.currentTimeMillis() - tev;
                countTested++;
                numberofOpsinWindow+= decomposition.getWindows().get(maximalIndex).v1.operators.size();
                if(unchangedSinks.containsAll(allSinks)){
                    break;
                }
            }
        }
        long perf = System.currentTimeMillis() - t0;
        System.out.println("AVG calling EV " + (evPerf / countTested));
        System.out.println("TOTAL calling EV " + evPerf);
        System.out.println("PERF of everything " + (perf - evPerf));
        System.out.println("NUMBER OF DECOMPOSITIONS EXPLORED " + countDecomp);
        System.out.println("NUMBER OF DECOMPOSITIONS TESTED " + countTested);
        System.out.println("AVG size of MCW " + numberofOpsinWindow);
        System.out.println("UNCHANGED " + unchangedSinks.size());
        System.out.println("=====================================");
    }

    @Override
    public Set<String> getEquivalentSinks() {
        long t0 = System.currentTimeMillis();
        if(allSinks.stream().allMatch(sink -> isInequivalent(sink))) {
            long inequiv = System.currentTimeMillis() - t0;
            System.out.println("inequivalence " + inequiv);
            System.out.println("=====================================");
            return unchangedSinks;
        }
        Decomposition initialDecomposition = initDecomposition();
        long mapping = System.currentTimeMillis() - t0;
        System.out.println("MAPPING " + mapping);
        exploreDecompositions(initialDecomposition);
        return unchangedSinks;
    }

}
