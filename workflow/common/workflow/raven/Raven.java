package edu.uci.ics.texera.workflow.common.workflow.raven;

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor;
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo;
import org.jooq.types.UInteger;

import java.util.*;

public class Raven {

    Map<Integer, V2Graph> workflowToV2Map = new HashMap<>();

    public List<Integer> getRankedVersions(UInteger wid, UInteger vid, WorkflowInfo.WorkflowDAG query) {
        // first construct the representation for the query
        Set<List<String>> representations = constructRepresentations(query);
        // then retrieve the V2 graph for the workflow
        if(!workflowToV2Map.containsKey(wid.intValue())) {
            workflowToV2Map.put(wid.intValue(), new V2Graph());
        }
        V2Graph workflowGraph = workflowToV2Map.get(wid.intValue());
        return workflowGraph.rankAndUpdate(representations, vid);
    }

    private Set<List<String>> constructRepresentations(WorkflowInfo.WorkflowDAG query) {
        Map<OperatorDescriptor, Schema[]> inputSchemaMap = constructInputSchemaMap(query);
        Set<List<String>> representations = new HashSet<>();
        // loop the sinks in the query, and get their schema and add them to the set
        query.getSinkOperators().foreach(sink -> representations.add(getCols(query.operators().get(sink).get(), inputSchemaMap)));
        return representations;
    }

    private List<String> getCols(OperatorDescriptor op, Map<OperatorDescriptor, Schema[]> inputSchemaMap) {
        return op.getOutputSchema(inputSchemaMap.get(op)).getAttributeNames();
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

}
