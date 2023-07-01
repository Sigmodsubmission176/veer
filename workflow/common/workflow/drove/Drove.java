package edu.uci.ics.texera.workflow.common.workflow.drove;

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor;
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo;
import java.util.*;
import java.util.stream.Collectors;

public class Drove {
    Set<String> unchangedSinks= new HashSet<>();
    WorkflowInfo.WorkflowDAG v1;
    WorkflowInfo.WorkflowDAG v2;
    EV ev;
    Set<String> allSinks = new HashSet<>();
    Map<OperatorDescriptor, Schema[]> inputSchemaMap = new HashMap<>();

    public Drove(EV ev, WorkflowInfo.WorkflowDAG v1, WorkflowInfo.WorkflowDAG v2) {
        this.ev = ev;
        this.v1 = v1;
        this.v2 = v2;
        inputSchemaMap = constructInputSchemaMap(v1);
        inputSchemaMap.putAll(constructInputSchemaMap(v2));
        v2.getSinkOperators().foreach(sink -> {
            if(v1.getSinkOperators().contains(sink))
            allSinks.add(sink);
            return null;
        });
    }

    private Map<OperatorDescriptor, Schema[]> constructInputSchemaMap(WorkflowInfo.WorkflowDAG originalDAG) {
        Map<OperatorDescriptor, Schema[]> inputSchema = new HashMap<>();
        Iterator<String> topologicalOrderIterator = originalDAG.jgraphtDag().iterator();
        topologicalOrderIterator.forEachRemaining(opID -> {
            OperatorDescriptor op = originalDAG.getOperator(opID);
            Schema outputSchema = null;
            try{
                if(op instanceof SourceOperatorDescriptor) {
                    outputSchema = op.getOutputSchema(new Schema[]{});
                }
                else {
                    outputSchema = op.getOutputSchema(inputSchema.get(op));
                }
            }
            catch (Exception e) {
                System.out.println(e.getMessage());
            }
            if (op instanceof SourceOperatorDescriptor) {
                inputSchema.put(op, new Schema[]{});
            }
            Schema finalOutputSchema = outputSchema;
            originalDAG.getDownstream(opID).foreach(downstream -> {
                if(!inputSchema.containsKey(downstream)) {
                    inputSchema.put(downstream, new Schema[originalDAG.getUpstream(downstream.operatorID()).size()]);
                }
                Schema[] schemas = inputSchema.get(downstream);
                for(int i = 0; i < schemas.length; i++) {
                    if(schemas[i] == null) {
                        schemas[i] = finalOutputSchema;
                        break;
                    }
                }
                return null;
            });
        });
        return inputSchema;
    }

    public void exploreDecompositions(Decomposition seed) {

        }

    public Set<String> getEquivalentSinks() {
        return null;
    }

    protected boolean isInequivalent(String sinkName) {
        if(!getCols(v1.getOperator(sinkName)).equals(getCols(v2.getOperator(sinkName))))
            return true;
        return false;
    }

    private List<String> getCols(OperatorDescriptor op) {
        return op.getOutputSchema(inputSchemaMap.get(op)).getAttributeNames();
    }

    public Decomposition initDecomposition() {
        Map<Mapping, Window> opToWindowMap = new HashMap<>();
        Map<Mapping, List<Mapping>> opToDownstreamMap = new HashMap<>();
        Map<Mapping, List<Mapping>> opToUpstreamMap = new HashMap<>();

        for(String sinkID: allSinks) {
            List<String> v1UpStreams = new ArrayList<>();
            List<String> v2UpStreams = new ArrayList<>();
            if (v2.operators().contains(sinkID)) {
                v2.getUpstream(sinkID).foreach(op -> v2UpStreams.add(op.operatorID()));
            }
            if (v1.operators().contains(sinkID)) {
                v1.getUpstream(sinkID).foreach(op -> v1UpStreams.add(op.operatorID()));
            }

            List<Mapping> operators = getMergedUpStreams(v1UpStreams, v2UpStreams);
            OperatorDescriptor opv1;
            OperatorDescriptor opv2;
            List<Mapping> downstreams;

            OperatorDescriptor opv1sink = v1.operators().contains(sinkID) ? v1.operators().get(sinkID).get() : null;
            OperatorDescriptor opv2sink = v2.operators().contains(sinkID) ? v2.operators().get(sinkID).get() : null;
            Set<String> reachable = new HashSet<>();
            reachable.add(sinkID);
            // construct window
            Window sinkwindow = new Window(new WorkflowDag(opv1sink), new WorkflowDag(opv2sink), reachable);

            Mapping sinkmapping = new Mapping(sinkID, sinkID);
            operators.forEach(mapping -> {
                if (!opToDownstreamMap.containsKey(mapping)) {
                    opToDownstreamMap.put(mapping, new ArrayList<>());
                }
                opToDownstreamMap.get(mapping).add(sinkmapping);
            });
            opToWindowMap.put(sinkmapping, sinkwindow);

            while (!operators.isEmpty()) {
                Mapping operatorsName = operators.remove(0);
                if (opToWindowMap.containsKey(operatorsName)) {
                    opToWindowMap.get(operatorsName).getReachableSinks().add(sinkID);
                    operators.addAll(opToUpstreamMap.get(operatorsName));
                } else {
                    List<Mapping> upstreams;
                    Set<String> reachableSinks = new HashSet<>();
                    reachableSinks.add(sinkID);
                    // get from both, check nulls
                    opv1 = v1.operators().contains(operatorsName.op1) ? v1.operators().get(operatorsName.op1).get() : null;
                    opv2 = v2.operators().contains(operatorsName.op2) ? v2.operators().get(operatorsName.op2).get() : null;
                    // construct window
                    Window window = new Window(new WorkflowDag(opv1), new WorkflowDag(opv2), reachableSinks);
                    // compare their equivalence
                    if (opv1 == null || opv2 == null) {
                        window.covering = true;
                    } else if (!opv1.equals(opv2)) {
                        window.covering = true;
                    }
                    window.supported = ev.isValid(window);
                    if (window.covering) {
                        if (!window.supported) {
                            return new Decomposition();
                        }
                    }
                    // get global downstreams from previous iterations
                    downstreams = opToDownstreamMap.get(operatorsName);
                    // update its upstreams in the map to add itself
                    if (downstreams != null) {
                        downstreams.forEach(downstream -> {
                            Window downstreamWindow = opToWindowMap.get(downstream);
                            downstreamWindow.upstreams.add(window);
                            window.downstreams.add(downstreamWindow);
                        });
                    }
                    opToWindowMap.put(operatorsName, window);
                    // get upstreams from both dags and check which ones are the merged global upstreams
                    List<String> v1grandUpStreams = new ArrayList<>();
                    List<String> v2grandUpStreams = new ArrayList<>();
                    if (v1.operators().contains(operatorsName.op1)) {
                        v1.getUpstream(operatorsName.op1).foreach(op -> v1grandUpStreams.add(op.operatorID()));
                    }
                    if (v2.operators().contains(operatorsName.op2)) {
                        v2.getUpstream(operatorsName.op2).foreach(op -> v2grandUpStreams.add(op.operatorID()));
                    }
                    upstreams = getMergedUpStreams(v1grandUpStreams, v2grandUpStreams);
                    opToUpstreamMap.put(operatorsName, upstreams);
                    // add this opName to the map for each upstream
                    upstreams.forEach(upstream -> {
                        if (opToWindowMap.containsKey(upstream)) {
                            Window visitedUpstream = opToWindowMap.get(upstream);
                            visitedUpstream.downstreams.add(window);
                            window.upstreams.add(visitedUpstream);
                        } else {
                            if (!opToDownstreamMap.containsKey(upstream)) {
                                opToDownstreamMap.put(upstream, new ArrayList<>());
                            }
                            opToDownstreamMap.get(upstream).add(operatorsName);
                        }
                        operators.add(upstream);
                    });
                }
            }
        }
        return new Decomposition(opToWindowMap.values().stream().collect(Collectors.toList()));
    }

    private List<Mapping> getMergedUpStreams(List<String> v1UpStreams, List<String> v2UpStreams) {
        if(v1UpStreams.containsAll(v2UpStreams) && v2UpStreams.containsAll(v1UpStreams)){
            return v1UpStreams.stream().map(upstream -> new Mapping(upstream, upstream)).collect(Collectors.toList());
        }
        // what are the possible mappings?
        // 1. one operator from v1 is deleted in v2
        // 2. one operator from v2 is added not in v1
        List<String> v1UpStreamsPreserved = new ArrayList<>(v1UpStreams);
        List<Mapping> mergedUpStreams = new ArrayList<>();
        List<String> tempMapping = new ArrayList<>();
        while(!v1UpStreams.isEmpty()) {
            String upstream = v1UpStreams.remove(0);
            // if the operator doesn't exist at all
            if(!v2.operators().contains(upstream))
            {
                tempMapping.add(upstream);
                continue;
            }
            // if it is also in the other workflow's upstreams, then they match
            if(v2UpStreams.contains(upstream)){
                mergedUpStreams.add(new Mapping(upstream, upstream));
                v2UpStreams.remove(upstream);
                continue;
            }
            // if the operator is in the workflow then if it is an ancestor to any of the other upstreams' then skip it otherwise add it
            if(v2UpStreams.stream().anyMatch(u -> v2.jgraphtDag().getAncestors(u).contains(upstream))){
                continue;
            }
            else{
                tempMapping.add(upstream);
            }
        }
        String op = "";
        while(!v2UpStreams.isEmpty()) {
            String upstream = v2UpStreams.remove(0);
            // if the operator is not in the workflow
            if(!v1.operators().contains(upstream)){
                // check the temp list for any swap mapping
                if(!tempMapping.isEmpty())
                {
                    op = tempMapping.remove(0);
                }
            }
            // if the operator is in the workflow then check if it is still a descendant of first or not
            else {
                if(v1UpStreamsPreserved.stream().anyMatch(u -> v1.jgraphtDag().getAncestors(u).contains(upstream))) {
                continue;
                }
                op = upstream;
            }
            mergedUpStreams.add(new Mapping(op, upstream));
            op = "";
        }
        // for any remaining unmatched ones from the temp list, add them without a corresponding match
        while(!tempMapping.isEmpty()){
            op = tempMapping.remove(0);
            String mapped = v2.operators().contains(op)? op : "";
            mergedUpStreams.add(new Mapping(op, mapped));
        }
        return mergedUpStreams;
    }

}
