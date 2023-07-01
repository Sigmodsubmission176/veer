package edu.uci.ics.texera.workflow.common.workflow.drove;

import com.microsoft.z3.Context;
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor;
import edu.uci.ics.texera.workflow.common.workflow.equitas.SymbolicColumn;
import edu.uci.ics.texera.workflow.operators.aggregate.AggregationFunction;
import edu.uci.ics.texera.workflow.operators.aggregate.SpecializedAverageOpDesc;
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpDesc;
import edu.uci.ics.texera.workflow.operators.hashJoin.JoinType;
import edu.uci.ics.texera.workflow.operators.source.scan.ScanSourceOpDesc;
import edu.uci.ics.texera.workflow.operators.source.sql.SQLSourceOpDesc;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Equitas implements EV {

    public static Map<String, SymbolicColumn> SYMBOLIC_AGGREGATE = new HashMap<>();
    final List<String> SUPPORTED_OPS = Arrays.asList("CSVScanSourceOpDesc",
            "ProjectionOpDesc","SpecializedFilterOpDesc", "HashJoinOpDesc",
            "SpecializedAverageOpDesc", "ProgressiveSinkOpDesc");

    Context z3Context = new Context();
    Map<String, OperatorDescriptor> tableColumnsMap = new HashMap<>();

    @Override
    public Set<String> getEquivalentSinks(WorkflowDag previousDag, WorkflowDag currentDag) {
        Set<String> unchangedSinks = new HashSet<>();
        SYMBOLIC_AGGREGATE.clear();
        tableColumnsMap.clear();
        clearSymbolicRepresentation(previousDag);
        clearSymbolicRepresentation(currentDag);
        previousDag.sinkOperators.forEach(sink -> {
            OperatorDescriptor operator = previousDag.operators.get(sink);
            constructPreviousSR(previousDag, operator);
        });

        currentDag.sinkOperators.forEach(sink -> {
            OperatorDescriptor currentOp = currentDag.operators.get(sink);
            constructCurrentSR(currentDag, currentOp);
            if(previousDag.operators.containsKey(sink)) {
                OperatorDescriptor previousSink = previousDag.operators.get(sink);
                if(currentOp.isEq(previousSink, z3Context)) {
                    unchangedSinks.add(sink);
                }
            }

        });
        return unchangedSinks;
    }

    private void clearSymbolicRepresentation(WorkflowDag dag) {
    dag.operators.forEach((s, operatorDescriptor) -> operatorDescriptor.clearSymbolicRepresentation());
    }

    private void constructCurrentSR(WorkflowDag currentDag, OperatorDescriptor operator) {
        if(currentDag.sourceOperators.contains(operator.operatorID())) {
            if (tableColumnsMap.containsKey(getSchemaSource(operator))) {
                OperatorDescriptor scannedPreviously = tableColumnsMap.get(getSchemaSource(operator));
                operator.constructSR(new OperatorDescriptor[]{scannedPreviously}, z3Context);
            }
            else {
                operator.constructSR(null, z3Context);
            }
        }
        else {
            List<OperatorDescriptor> upstreamsList = currentDag.getUpstream(operator.operatorID());
            OperatorDescriptor[] upstreams = upstreamsList.toArray(new OperatorDescriptor[upstreamsList.size()]);
            Arrays.stream(upstreams).forEach(upstream -> {
            if(upstream.isSymbolicNotConstructed())
                constructCurrentSR(currentDag, upstream);
            });
            operator.constructSR(upstreams, z3Context);
        }
    }

    private void constructPreviousSR(WorkflowDag previousDag, OperatorDescriptor operator) {
        if(previousDag.sourceOperators.contains(operator.operatorID())) {
            operator.constructSR(null, z3Context);
            tableColumnsMap.put(getSchemaSource(operator), operator);
        }
        else {
            List<OperatorDescriptor> upstreamsList = previousDag.getUpstream(operator.operatorID());
            OperatorDescriptor[] upstreams = upstreamsList.toArray(new OperatorDescriptor[upstreamsList.size()]);
            Arrays.stream(upstreams).forEach(upstream -> {
                if(upstream.isSymbolicNotConstructed())
                    constructPreviousSR(previousDag, upstream);
            });
            operator.constructSR(upstreams, z3Context);
        }
    }

    private String getSchemaSource(OperatorDescriptor descriptor) {
        String descriptorClass = descriptor.getClass().getSimpleName();
        String schemaIdentifier = "";
        if (Arrays.asList("CSVScanSourceOpDesc", "ParallelCSVScanSourceOpDesc", "JSONLScanSourceOpDesc").contains(descriptorClass)){
            ScanSourceOpDesc scan = (ScanSourceOpDesc) descriptor;
            schemaIdentifier = scan.filePath().get();
        }
        else if(Arrays.asList("AsterixDBSourceOpDesc", "MySQLSourceOpDesc", "PostgreSQLSourceOpDesc").contains(descriptorClass)) {
            SQLSourceOpDesc sql = (SQLSourceOpDesc) descriptor;
            schemaIdentifier = sql.database() + " " + sql.table();
        }
        return schemaIdentifier;
    }

    @Override
    public boolean isValid(Window window) {
        // 1. check for any unsupported op
        // 2. if agg or outerjoin number in the pair needs to be the same
        // 3. if agg ( count, needs all upstreams to be SPJ)
        short[] aggCount = {0, 0};
        short[] ojCount = {0, 0};
        AtomicBoolean valid = new AtomicBoolean(true);
        window.v1.operators.values().forEach(operator ->
        {
            String opType = operator.getClass().getSimpleName();
            if(!SUPPORTED_OPS.contains(opType)) {
                valid.set(false);
                return;
            }
            if(opType.equals("SpecializedAverageOpDesc")) {
                aggCount[0]++;
                if(!aggregateIsValid(operator, window.v1)) {
                    valid.set(false);
                    return;
                }
            }
            if(opType.equals("HashJoinOpDesc")) {
                HashJoinOpDesc hashJoin = (HashJoinOpDesc) operator;
                if(hashJoin.joinType() != JoinType.INNER) {
                    ojCount[0]++;
                }
            }
        });
        if(!valid.get()){
            return false;
        }

        window.v2.operators.values().forEach(operator ->
        {
            String opType = operator.getClass().getSimpleName();
            if(!SUPPORTED_OPS.contains(opType)) {
                valid.set(false);
                return;
            }
            if(opType.equals("SpecializedAverageOpDesc")) {
                aggCount[1]++;
                if(!aggregateIsValid(operator, window.v2)) {
                    valid.set(false);
                    return;
                }
            }
            if(opType.equals("HashJoinOpDesc")) {
                HashJoinOpDesc hashJoin = (HashJoinOpDesc) operator;
                if(hashJoin.joinType() != JoinType.INNER) {
                    ojCount[1]++;
                }
            }
        });
        if(aggCount[0] != aggCount[1] || ojCount[0] != ojCount[1]) {
            return false;
        }
        return valid.get();
    }

    private boolean aggregateIsValid(OperatorDescriptor operator, WorkflowDag dag) {
        AtomicBoolean valid = new AtomicBoolean(true);
        SpecializedAverageOpDesc aggregate = (SpecializedAverageOpDesc) operator;
        if(aggregate.aggFunction() == AggregationFunction.AVERAGE ||
                aggregate.aggFunction() == AggregationFunction.COUNT ||
                aggregate.aggFunction() == AggregationFunction.SUM) {
            List<OperatorDescriptor> allUpstream = dag.getAllUpstreams(operator.operatorID());
            allUpstream.forEach(upstream -> {
                String upstreamOpType = upstream.getClass().getSimpleName();
                if (!(upstreamOpType.equals("CSVScanSourceOpDesc")
                        || upstreamOpType.equals("ProjectionOpDesc")
                        || upstreamOpType.equals("SpecializedFilterOpDesc") ||
                        upstreamOpType.equals("HashJoinOpDesc"))) {
                    valid.set(false);
                    return;
                }
                if(upstreamOpType.equals("HashJoinOpDesc")) {
                    HashJoinOpDesc hashJoin = (HashJoinOpDesc) upstream;
                    if(hashJoin.joinType() != JoinType.INNER) {
                        valid.set(false);
                        return;
                    }
                }
            });
        }
        return valid.get();
    }
}