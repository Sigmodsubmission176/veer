package edu.uci.ics.texera.workflow.operators.filter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.microsoft.z3.Context;
import edu.uci.ics.amber.engine.common.Constants;
import edu.uci.ics.texera.workflow.common.metadata.InputPort;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.metadata.OutputPort;
import edu.uci.ics.texera.workflow.common.operators.OneToOneOpExecConfig;
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor;
import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpDesc;
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;
import edu.uci.ics.texera.workflow.common.workflow.equitas.z3Utility;
import java.util.*;

import static java.util.Collections.singletonList;
import static scala.collection.JavaConverters.asScalaBuffer;
import static scala.collection.JavaConverters.mapAsJavaMap;

public class SpecializedFilterOpDesc extends FilterOpDesc {

    @JsonProperty(value = "predicates", required = true)
    @JsonPropertyDescription("multiple predicates in OR")
    public List<FilterPredicate> predicates;

    @Override
    public OneToOneOpExecConfig operatorExecutor(OperatorSchemaInfo operatorSchemaInfo) {
        return new OneToOneOpExecConfig(
                operatorIdentifier(),
                worker -> new SpecializedFilterOpExec(this),
                Constants.currentWorkerNum()
        );
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "Filter",
                "Performs a filter operation",
                OperatorGroupConstants.SEARCH_GROUP(),
                asScalaBuffer(singletonList(new InputPort("", false))).toList(),
                asScalaBuffer(singletonList(new OutputPort(""))).toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
//        if (!super.equals(o)) return false;
        SpecializedFilterOpDesc that = (SpecializedFilterOpDesc) o;
        return Objects.equals(predicates, that.predicates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), predicates);
    }

    @Override
    public void constructSymbolicCondition(OperatorDescriptor[] upstreamOperators, Context z3Context) {
//        List<BoolExpr> assign = new ArrayList<>();
        this.setSymbolicCondition(z3Utility.joinSymbolicCoumns(upstreamOperators, z3Utility.getNodeSymbolicColumn(false, true, mapAsJavaMap(this.getSymbolicColumns()), this.predicates,z3Context), z3Context));
//        assign.add(upstreamOperators[0].getVariableConstrainst());
//        assign.add(this.getSymbolicCondition());
//        this.setVariableConstrainst(z3Utility.mkAnd(assign,z3Context));
        }

}
