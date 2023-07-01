package edu.uci.ics.texera.workflow.common.workflow.drove;

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor;
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo;
import edu.uci.ics.texera.workflow.operators.source.scan.csv.CSVScanSourceOpDesc;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class WorkflowDag {
    public Map<String, OperatorDescriptor> operators = new HashMap<>();

DirectedAcyclicGraph<String, DefaultEdge> jgraphtDag = new DirectedAcyclicGraph<>(DefaultEdge.class);

List<String> sourceOperators = new ArrayList<>();
    List<String> sinkOperators = new ArrayList<>();

    public WorkflowDag(WorkflowDag other) {
        sourceOperators = new ArrayList<>(other.sourceOperators);
        sinkOperators = new ArrayList<>(other.sinkOperators);
        operators = new HashMap<>(other.operators);
        jgraphtDag = (DirectedAcyclicGraph<String, DefaultEdge>) other.jgraphtDag.clone();
    }

    public WorkflowDag(OperatorDescriptor operator) {
        if(operator != null) {
            sourceOperators.add(operator.operatorID());
            sinkOperators.add(operator.operatorID());
            jgraphtDag.addVertex(operator.operatorID());
            operators.put(operator.operatorID(), operator);
        }
    }

    public List<String> getSinkOperators() {
        return sinkOperators;
    }

    public List<String> getSourceOperators() {
        return sourceOperators;
    }

    public List<OperatorDescriptor> getUpstream(String operatorID) {
        List<OperatorDescriptor> upstreams = new ArrayList<>();
        jgraphtDag
                .incomingEdgesOf(operatorID)
                .forEach(e -> upstreams.add(operators.get(jgraphtDag.getEdgeSource(e))));
        return upstreams;
    }

    public List<OperatorDescriptor> getAllUpstreams(String operatorID) {
        List<OperatorDescriptor> allUpstreams = new ArrayList<>();
        Set<String> ancestors = jgraphtDag.getAncestors(operatorID);
        ancestors.forEach(ancestor -> allUpstreams.add(operators.get(ancestor)));
        return allUpstreams;
    }

    public WorkflowDag deepCopy() {
        WorkflowDag copy = new WorkflowDag(this);
        return copy;
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

    public void addVirtualSourcesAndSinks(WorkflowInfo.WorkflowDAG originalDAG) {
        // construct schema map
        Map<OperatorDescriptor, Schema[]> inputSchemas = constructInputSchemaMap(originalDAG);
        Set<String> sourcesToBeRemoved = new HashSet<>();
        Set<String> sourcesToBeAdded = new HashSet<>();
        sourceOperators.forEach(source -> {
            if(operators.get(source).getClass().getSimpleName().equals("CSVScanSourceOpDesc"))
            {
                return;
            }
                originalDAG.getUpstream(source).foreach(up ->
                {
                    if(!operators.containsKey(up.operatorID())) {
                        OperatorDescriptor replacedSource = getOriginalSource(up, originalDAG, sourcesToBeAdded, inputSchemas);
                            sourcesToBeRemoved.add(source);
                            jgraphtDag.addEdge(replacedSource.operatorID(), source);
                    }
                    return null;
                });
                });
        sourceOperators.removeAll(sourcesToBeRemoved);
        sourceOperators.addAll(sourcesToBeAdded);
        Set<String> sinksToBeAdded = new HashSet<>();
        Set<String> sinksToBeRemoved = new HashSet<>();
        // then do the same for sinks
        sinkOperators.forEach(sink -> {
            List<String> reachableSinks = originalDAG.jgraphtDag().getDescendants(sink)
                    .stream().filter(descendant -> originalDAG.sinkOperators().contains(descendant))
                    .collect(Collectors.toList());
            reachableSinks.forEach(originalSink -> {
                // check if it is not added already
                if(!operators.containsKey(originalSink)) {
                    // get the opDesc, then add it as a vertex, as an edge, and in ops map
                    OperatorDescriptor op = originalDAG.getOperator(originalSink);
                    operators.put(originalSink, op);
                    sinksToBeAdded.add(originalSink);
                    jgraphtDag.addVertex(originalSink);
                }
                sinksToBeRemoved.add(sink);
                jgraphtDag.addEdge(sink, originalSink);
            });
        });
        sinkOperators.removeAll(sinksToBeRemoved);
        sinkOperators.addAll(sinksToBeAdded);
    }

    private OperatorDescriptor getOriginalSource(OperatorDescriptor source, WorkflowInfo.WorkflowDAG originalDAG, Set<String> sourcesToBeAdded, Map<OperatorDescriptor, Schema[]> inputSchemas) {
        final CSVScanSourceOpDesc originalSource = new CSVScanSourceOpDesc();
        originalSource.setSchema(Schema.newBuilder().build());
        originalSource.appendSchema(originalDAG.operators().get(source.operatorID()).get().getOutputSchema(inputSchemas.get(source)));
                originalSource.setFilePath(source.operatorID());
                // get the opDesc, then add it as a vertex, as an edge, and in ops map
                operators.put(originalSource.operatorID(), originalSource);
                sourcesToBeAdded.add(originalSource.operatorID());
                jgraphtDag.addVertex(originalSource.operatorID());
        return originalSource;
    }
    public void mergeLeft(WorkflowDag dag, WorkflowInfo.WorkflowDAG originalDAG) {
        Set<String> sourcesToBeRemoved = new HashSet<>();
        Set<String> sinksToBeAdded = new HashSet<>();
        Set<String> sinksToBeRemoved = new HashSet<>();
        dag.sinkOperators.forEach(sink -> {
            // test if this sink has to remain as a sink because it is a replicate
            AtomicBoolean remainsSink = new AtomicBoolean(false);
            originalDAG.getDownstream(sink).foreach(down -> {
                if(!operators.containsKey(down.operatorID()) && !dag.operators.containsKey(down.operatorID())) { // if there is a downstream not in this yet
                    remainsSink.set(true);
                    if (!sinkOperators.contains(sink)) { // if not in sink list yet
                        sinksToBeAdded.add(sink);
                    }
                }
                return null;
            });
            // if all downstreams are already in DAG then just delete it from sinks
            if(!remainsSink.get()){
                sinksToBeRemoved.add(sink);
            }
            jgraphtDag.addVertex(sink);
        });
        sourceOperators.forEach(source -> {
            AtomicBoolean remainsSource = new AtomicBoolean(false);
            AtomicBoolean remainsSink = new AtomicBoolean(false);
            if(originalDAG.getUpstream(source).isEmpty()) {
                remainsSource.set(true);
            }
            if(originalDAG.getDownstream(source).isEmpty()) {
                remainsSink.set(true);
            }
            originalDAG.getUpstream(source).foreach(upstream -> {
                if(dag.sinkOperators.contains(upstream.operatorID())) {
                    // get its upstreams from original
                    // for every sink in window, check if it is in upstreams, then connect them
                    jgraphtDag.addEdge(upstream.operatorID(), source);
                }
                else{
                    if(!operators.containsKey(upstream.operatorID())) {
                        remainsSource.set(true);
                    }
                }
                return null;
            });
            originalDAG.getDownstream(source).foreach(downstream -> {
                if(dag.operators.containsKey(downstream.operatorID())) {
                    // get its upstreams from original
                    // for every sink in window, check if it is in upstreams, then connect them
                    if(!jgraphtDag.containsVertex(downstream.operatorID())) {
                        jgraphtDag.addVertex(downstream.operatorID());
                    }
                    jgraphtDag.addEdge(source, downstream.operatorID());
                }
                else{
                        remainsSink.set(true);
                }
                return null;
            });

            if(!remainsSource.get()) {
                sourcesToBeRemoved.add(source);
            }
            if(!remainsSink.get()) {
                sinksToBeRemoved.add(source);
            }
        });
        // after done with the integration, add all of the upstreams of those sinks of the window to the jgrapht
        List<String> ops = new ArrayList<>(dag.sinkOperators);
        while(!ops.isEmpty()) {
            String op = ops.remove(0);
            dag.jgraphtDag.incomingEdgesOf(op).forEach(e -> {
                String upstream = dag.jgraphtDag.getEdgeSource(e);
                if(!jgraphtDag.containsVertex(upstream)) {
                    jgraphtDag.addVertex(upstream);
                }
                jgraphtDag.addEdge(upstream, op);
                ops.add(upstream);
            });
        }
            sourceOperators.removeAll(sourcesToBeRemoved);
            sourceOperators.addAll(dag.sourceOperators);
        if(sinkOperators.isEmpty()) {
            sinksToBeAdded.addAll(sourceOperators);
        }
        sinkOperators.removeAll(sinksToBeRemoved);
        sinkOperators.addAll(sinksToBeAdded);
        // if dag is a replicate, then its other paths should also be added
        operators.putAll(dag.operators);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkflowDag that = (WorkflowDag) o;
        return isEqualGraphs(jgraphtDag, that.jgraphtDag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jgraphtDag);
    }

    private boolean isEqualGraphs(DirectedAcyclicGraph<String, DefaultEdge> jgraphtDag1, DirectedAcyclicGraph<String, DefaultEdge> jgraphtDag2) {
        return Objects.equals(jgraphtDag1.vertexSet(), jgraphtDag2.vertexSet()) && isEqualEdges(jgraphtDag1.edgeSet(), jgraphtDag2.edgeSet(), jgraphtDag2);
    }

    private boolean isEqualEdges(Set<DefaultEdge> edges1, Set<DefaultEdge> edges2, DirectedAcyclicGraph<String, DefaultEdge> dag) {
       Iterator<DefaultEdge> iterator = edges1.iterator();
       DefaultEdge edge;
       while (iterator.hasNext()){
           edge = iterator.next();
           if(!isEdgeIn(edge, edges2, dag)) {
               return false;
           }
       }
        iterator = edges2.iterator();
        while (iterator.hasNext()){
            edge = iterator.next();
            if(!isEdgeIn(edge, edges1, jgraphtDag)) {
                return false;
            }
        }
       return true;
    }

    private boolean isEdgeIn(DefaultEdge edge, Set<DefaultEdge> edges, DirectedAcyclicGraph<String, DefaultEdge> dag) {
        return edges.stream().anyMatch(e -> (jgraphtDag.getEdgeSource(e).equals(dag.getEdgeSource(edge)) && jgraphtDag.getEdgeTarget(e).equals(dag.getEdgeTarget(edge))));
    }
}