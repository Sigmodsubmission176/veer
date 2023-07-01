package edu.uci.ics.texera.workflow.common.workflow.raven;

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor;
import edu.uci.ics.texera.workflow.common.workflow.drove.EV;
import edu.uci.ics.texera.workflow.common.workflow.drove.WorkflowDag;
import javafx.util.Pair;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class CachedSubDAGs {
    short equivalenceClassCounter = 0;
    public Map<String, RecyclerNode> recyclerNodes = new HashMap<>();
    DirectedAcyclicGraph<String, DefaultEdge> recyclerGraph = new DirectedAcyclicGraph<>(DefaultEdge.class);
    List<String> sourceNodes = new ArrayList<>();

    public CachedSubDAGs() {
    }

    public Set<String> getEquivalentSinks(EV ev, WorkflowDag v1, WorkflowDag v2) {
        long t0 = System.currentTimeMillis();
        HashMap<String, Pair<Boolean, RecyclerNode>> query1MatchToGraph = new HashMap<>();
        HashMap<String, Pair<Boolean, RecyclerNode>> query2MatchToGraph = new HashMap<>();
        v2.getSinkOperators().forEach(sink -> {
            matchDAG(v2, v2.operators.get(sink), query2MatchToGraph);
            // if equivalence class of recycler node for sink is -1 then assign it a random number
            if (query2MatchToGraph.get(sink).getValue().equivalenceClass == -1) {
                query2MatchToGraph.get(sink).getValue().equivalenceClass = equivalenceClassCounter++;
            }
        });
        v1.getSinkOperators().forEach(sink -> {
            matchDAG(v1, v1.operators.get(sink), query1MatchToGraph).getKey();
            if (query1MatchToGraph.get(sink).getValue().equivalenceClass == -1) {
                query1MatchToGraph.get(sink).getValue().equivalenceClass = equivalenceClassCounter++;
            }
        });

        boolean callEvFlag = false;
        Set<String> equivalentSinks = new HashSet<>();
        for (Map.Entry<String, Pair<Boolean, RecyclerNode>> entry : query2MatchToGraph.entrySet()) {
            if (query1MatchToGraph.containsKey(entry.getKey())) {
                if (entry.getValue().getValue().equivalenceClass
                        == query1MatchToGraph.get(entry.getKey()).getValue().equivalenceClass) {
                    equivalentSinks.add(entry.getKey());
                } else {
                    // if they were tested before then just ignore them,
                    // otherwise break this loop and call EV
                    if(!entry.getValue().getValue().checkedClasses.contains(
                            query1MatchToGraph.get(entry.getKey()).getValue().equivalenceClass)) {
                        callEvFlag = true;
                        break;
                    }
                }
            }
        }
        if(callEvFlag){
            equivalentSinks = ev.getEquivalentSinks(v1, v2);
            // then loop set of equivalent sinks and update their equivalence classes and update the matrix
            Set<String> finalEquivalentSinks = equivalentSinks;
            v2.getSinkOperators().forEach(sink -> {
                int v2EC = query2MatchToGraph.get(sink).getValue().equivalenceClass;
                // if it is in the set of equivalent then update the class of the other to be same as v2
                if(finalEquivalentSinks.contains(sink)) {
                    query1MatchToGraph.get(sink).getValue().equivalenceClass = v2EC;
                }
                // if it is not then if it exists in v1 then update both classes to include other was checked
                else {
                    if(query1MatchToGraph.containsKey(sink)) {
                        RecyclerNode v1Node = query1MatchToGraph.get(sink).getValue();
                        v1Node.checkedClasses.add(v2EC);
                        query2MatchToGraph.get(sink).getValue().checkedClasses.add(v1Node.equivalenceClass);
                    }
                }
            });
        }
        System.out.println("WINDOW REUSE " + (System.currentTimeMillis() - t0));
        return equivalentSinks;
    }

    /**
     * this function takes in a single sink and recursively does the matching
     * @param query
     * @param operator
     * @return flag if the DAG is matched and the name of the node it matched with
     */
    private Pair<Boolean, RecyclerNode> matchDAG(WorkflowDag query, OperatorDescriptor operator,
                                                 Map<String, Pair<Boolean, RecyclerNode>> queryMatchToGraph) {
        if(queryMatchToGraph.containsKey(operator.operatorID())) {
            return queryMatchToGraph.get(operator.operatorID()); // this is because if
            // an upstream node is already matched by another child then just return its matching
        }
        Pair<Boolean, RecyclerNode> result;
        if(query.getSourceOperators().contains(operator.operatorID())) {
            RecyclerNode matched = findMatch(operator, sourceNodes.stream().map(source ->
                    recyclerNodes.get(source)).collect(Collectors.toList()));
            boolean matchedFlag = true;
            String nodeName = operator.operatorID();
            // if not matched, then update the recycler graph
            if(matched == null) {
                matchedFlag = false;
                if(recyclerGraph.containsVertex(operator.operatorID()))
                    nodeName = UUID.randomUUID().toString();
                sourceNodes.add(nodeName);
                recyclerGraph.addVertex(nodeName);
                matched = new RecyclerNode(operator, -1, nodeName);
                recyclerNodes.put(nodeName, matched);
            }
            result = new Pair<>(matchedFlag, matched);
            queryMatchToGraph.put(operator.operatorID(), result);
            return result;
        }
        else {
            AtomicBoolean allMatched = new AtomicBoolean(true);
            List<RecyclerNode> matchedOperators = new ArrayList<>();
            query.getUpstream(operator.operatorID()).forEach(upstream -> {
                Pair<Boolean, RecyclerNode> matched = matchDAG(query, upstream, queryMatchToGraph);
                if(!matched.getKey()){
                    allMatched.set(false);
                }
                matchedOperators.add(matched.getValue());
            });
            if(allMatched.get()) {
                // perform check that operator is equal
                // for each upstream get its downstreams
                Set<RecyclerNode> siblings = new HashSet<>();
                matchedOperators.forEach(upstream ->
                        recyclerGraph
                                .outgoingEdgesOf(upstream.name)
                                .forEach(e -> siblings.add(recyclerNodes.get(recyclerGraph.getEdgeTarget(e)))));
                RecyclerNode matched = findMatch(operator, siblings.stream().collect(Collectors.toList()));
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
     * @return the new node in the recycler graph
     */
    private RecyclerNode updateRecyclerGraph(OperatorDescriptor newNode, List<RecyclerNode> upstreams) {
        String newName = newNode.operatorID();
        if(recyclerGraph.containsVertex(newNode.operatorID()))
            newName = UUID.randomUUID().toString();
        recyclerGraph.addVertex(newName);
        RecyclerNode recyclerNode = new RecyclerNode(newNode, -1, newName);
        recyclerNodes.put(newName, recyclerNode);
        String finalNewName = newName;
        upstreams.forEach(upstream -> recyclerGraph.addEdge(upstream.name, finalNewName));
        return recyclerNode ;
    }

    /**
     * takes a node from the query and matches it with any of the nodes in the recycler graph
     * @param queryNode
     * @param nodes
     * @return the node in the recycler graph that matches the query node or null if no match
     */
    private RecyclerNode findMatch(OperatorDescriptor queryNode, List<RecyclerNode> nodes) {
        for(RecyclerNode node: nodes) {
            if(node.operator.equals(queryNode)) {
                return node;
            }
        }
        return null;
    }
}
