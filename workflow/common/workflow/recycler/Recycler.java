package edu.uci.ics.texera.workflow.common.workflow.recycler;

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor;
import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo;
import javafx.util.Pair;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class Recycler {
    public Map<String, OperatorDescriptor> operators = new HashMap<>();
    DirectedAcyclicGraph<String, DefaultEdge> recyclerGraph = new DirectedAcyclicGraph<>(DefaultEdge.class);
    List<String> sourceOperators = new ArrayList<>();
    HashMap<String, Pair<Boolean, String>> queryMatchToGraph = new HashMap<>();
    final List<String> SUPPORTED_OPS = Arrays.asList("CSVScanSourceOpDesc",
            "ProjectionOpDesc","SpecializedFilterOpDesc", "HashJoinOpDesc",
            "SpecializedAverageOpDesc", "ProgressiveSinkOpDesc");
    /**
     * @param query DAG
     * @return a set of the sinks names
     * from the query that got matched with the recycler graph
     */
    public Set<String> matchAndUpdate(WorkflowInfo.WorkflowDAG query) {
        Set<String> sinks = new HashSet<>();
        if (query.operators().values().exists(operator -> !SUPPORTED_OPS.contains(operator.getClass().getSimpleName()))) {
            return sinks;
        }
        queryMatchToGraph.clear();
        query.getSinkOperators().foreach(sink -> {
            if(matchDAG(query, query.getOperator(sink)).getKey()) {
                sinks.add(sink);
                System.out.println("MATCHED " + sink);
            }
            return null;
        });
        return sinks;
    }

    /**
     * this function takes in a single sink and recursively does the matching
     * @param query
     * @param operator
     * @return flag if the DAG is matched and the name of the node it matched with
     */
    private Pair<Boolean, String> matchDAG(WorkflowInfo.WorkflowDAG query, OperatorDescriptor operator) {
        if(queryMatchToGraph.containsKey(operator.operatorID())) {
            return queryMatchToGraph.get(operator.operatorID()); // this is because if
            // an upstream node is already matched by another child then just return its matching
        }
        Pair<Boolean, String> result;
        if(query.sourceOperators().contains(operator.operatorID())) {
            String matched = findMatch(operator, sourceOperators.stream().map(source ->
                    operators.get(source)).collect(Collectors.toList()));
            boolean matchedFlag = true;
            String opName = operator.operatorID();
            // if not matched, then update the recycler graph
            if(matched == null) {
                matchedFlag = false;
                if(recyclerGraph.containsVertex(operator.operatorID()))
                    opName = UUID.randomUUID().toString();
                sourceOperators.add(opName);
                recyclerGraph.addVertex(opName);
                operators.put(opName, operator);
            }
            result = new Pair<>(matchedFlag, opName);
            queryMatchToGraph.put(operator.operatorID(), result);
            return result;
        }
        else {
            AtomicBoolean allMatched = new AtomicBoolean(true);
            List<String> matchedOperators = new ArrayList<>();
            query.getUpstream(operator.operatorID()).foreach(upstream -> {
                Pair<Boolean, String> matched = matchDAG(query, upstream);
                if(!matched.getKey()){
                    allMatched.set(false);
                }
                matchedOperators.add(matched.getValue());
                return null;
            });
            if(allMatched.get()) {
                // perform check that operator is equal
                // for each upstream get its downstreams
                Set<OperatorDescriptor> siblings = new HashSet<>();
                matchedOperators.forEach(upstream ->
                    recyclerGraph
                            .outgoingEdgesOf(upstream)
                            .forEach(e -> siblings.add(operators.get(recyclerGraph.getEdgeTarget(e)))));
                String matched = findMatch(operator, siblings.stream().collect(Collectors.toList()));
                // if op is matched then return the match
                if(matched != null) {
                    result = new Pair<>(true, matched);
                    queryMatchToGraph.put(operator.operatorID(), result);
                    return result;
                }
            }
            result = new Pair<>(false, updateRecyclerGraph(operator, matchedOperators));
            queryMatchToGraph.put(operator.operatorID(), result);
            return result;
        }
    }

    /**
     * this function adds a new node to the recycler graph
     * @param newNode
     * @param upstreams
     * @return name of the new node in the recycler graph
     */
    private String updateRecyclerGraph(OperatorDescriptor newNode, List<String> upstreams) {
        String newName = newNode.operatorID();
        if(recyclerGraph.containsVertex(newNode.operatorID()))
            newName = UUID.randomUUID().toString();
        recyclerGraph.addVertex(newName);
        operators.put(newName, newNode);
        String finalNewName = newName;
        upstreams.forEach(upstream -> recyclerGraph.addEdge(upstream, finalNewName));
        return newName;
    }

    /**
     * takes a node from the query and matches it with any of the nodes in the recycler graph
     * @param queryNode
     * @param nodes
     * @return the name of the node in the recycler graph that matches the query node or null if no match
     */
    private String findMatch(OperatorDescriptor queryNode, List<OperatorDescriptor> nodes) {
        for(OperatorDescriptor node: nodes) {
            if(node.equals(queryNode)) {
                return node.operatorID();
            }
        }
        return null;
    }
}
