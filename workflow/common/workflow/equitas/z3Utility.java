package edu.uci.ics.texera.workflow.common.workflow.equitas;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.microsoft.z3.*;
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor;
import edu.uci.ics.texera.workflow.operators.filter.ComparisonType;
import edu.uci.ics.texera.workflow.operators.filter.FilterPredicate;
import org.scalactic.Bool;

public class z3Utility {

    static public boolean isConditionEq(BoolExpr condition1, BoolExpr condition2, Context z3Context) {
        BoolExpr equation = z3Context.mkNot(z3Context.mkEq(condition1,condition2));
        Solver s = z3Context.mkSolver();
        s.add(equation);
        return s.check() == Status.UNSATISFIABLE;
    }

    static public boolean symbolicOutputEqual(BoolExpr conditions, SymbolicColumn[] list1, SymbolicColumn[] list2, Context z3Context){
        if(list1.length == list2.length) {
            List<BoolExpr> columnEqs = new ArrayList<>();
            for(int i=0; i<list1.length; i++){
                BoolExpr columnEq = symbolicColumnEq(list1[i],list2[i],z3Context);
                if (columnEq != null){
                    columnEqs.add(columnEq);
                }
            }
            if (columnEqs.isEmpty()){
                return true;
            }
            BoolExpr notEq = (BoolExpr) z3Context.mkNot(z3Utility.mkAnd(columnEqs,z3Context)).simplify();
            Solver s = z3Context.mkSolver();
            s.add((BoolExpr) z3Context.mkAnd(conditions, notEq).simplify());
            return s.check() == Status.UNSATISFIABLE ;
        }
        return false;
    }

    static public SymbolicColumn joinSymbolicCoumns(OperatorDescriptor[] upstreamOperators, SymbolicColumn predicate, Context z3Context) {
        List<BoolExpr> resultValues = new ArrayList<>();
        List<BoolExpr> nullValues = new ArrayList<>();
        List<BoolExpr> nullSymbol = new ArrayList<>();
        for (int i = 0; i < upstreamOperators.length; i++){
            BoolExpr result = (BoolExpr) upstreamOperators[i].symbolicCondition().getSymbolicValue();
            resultValues.add(result);
            BoolExpr symbolicNull = upstreamOperators[i].symbolicCondition().getSymbolicNull();
            nullValues.add(z3Context.mkOr(symbolicNull,result));
            nullSymbol.add(symbolicNull);
        }
        resultValues.add((BoolExpr) predicate.getSymbolicValue());
        nullValues.add(z3Context.mkOr(predicate.getSymbolicNull(), (BoolExpr) predicate.getSymbolicValue()));
        nullSymbol.add(predicate.getSymbolicNull());
        BoolExpr outputValue = mkAnd(resultValues,z3Context);
        BoolExpr outputNull = (BoolExpr) z3Context.mkAnd(mkAnd(nullValues,z3Context),mkOr(nullSymbol,z3Context)).simplify();
        return (new SymbolicColumn(outputValue,outputNull,z3Context));
    }
    static public SymbolicColumn getNodeSymbolicColumn(boolean conjunctiveFlag, boolean constantFlag, Map<String, SymbolicColumn> symbolicColumns, Collection<FilterPredicate> operands, Context z3Context){
        List<BoolExpr> resultValues = new ArrayList<>();
        List<BoolExpr> nullValues = new ArrayList<>();
        List<BoolExpr> nullSymbol = new ArrayList<>();
        for(FilterPredicate operand:operands){
            SymbolicColumn convertedCondition = convertCondition(constantFlag, symbolicColumns, operand,z3Context);
            BoolExpr result = (BoolExpr)convertedCondition.getSymbolicValue();
            BoolExpr symbolicNull = convertedCondition.getSymbolicNull();
            resultValues.add(result);
            if(conjunctiveFlag){
                nullValues.add(z3Context.mkOr(symbolicNull,result));
            }
            else {
                nullValues.add(z3Context.mkOr(symbolicNull,z3Context.mkNot(result)));
            }
            nullSymbol.add(symbolicNull);
//            assignConstraints.addAll(convertedCondition.getAssignConstrains());
        }
        BoolExpr outputValue = conjunctiveFlag ? mkAnd(resultValues,z3Context): mkOr(resultValues, z3Context);
        BoolExpr outputNull = (BoolExpr) z3Context.mkAnd(mkAnd(nullValues,z3Context),mkOr(nullSymbol,z3Context)).simplify();
        return (new SymbolicColumn(outputValue,outputNull,z3Context));
    }

    public static BoolExpr mkOr(List<BoolExpr> constraints,Context z3Context){
        BoolExpr[] orC = new BoolExpr[constraints.size()];
        constraints.toArray(orC);
        return (BoolExpr) z3Context.mkOr(orC).simplify();
    }

    static private SymbolicColumn convertCondition(boolean constantFlag, Map<String, SymbolicColumn> symbolicColumns, FilterPredicate predicate, Context z3Context) {
        SymbolicColumn leftOperand = symbolicColumns.get(predicate.attribute);

//        this.assignConstraints.addAll(leftConverter.getAssignConstrains());
//        this.assignConstraints.addAll(rightConverter.getAssignConstrains());

        switch (predicate.condition) {
            case IS_NULL:
                return new SymbolicColumn(leftOperand.getSymbolicNull(),z3Context.mkFalse(),z3Context);
            case IS_NOT_NULL:
                return new SymbolicColumn(z3Context.mkNot(leftOperand.getSymbolicNull()),z3Context.mkFalse(),z3Context);
            default: {
                SymbolicColumn rightOperand;
                if(constantFlag) {
                  rightOperand = new SymbolicColumn(z3Context.mkConst(predicate.value, SymbolicColumn.getSortBasedOnType(z3Context, leftOperand.getType())), z3Context.mkFalse(), z3Context, leftOperand.getType());
                }
                else{
                    rightOperand = symbolicColumns.get(predicate.value);
                }
                Expr outputValue = buildCompareResult(z3Context, leftOperand.getSymbolicValue(), rightOperand.getSymbolicValue(), predicate.condition);
                BoolExpr outputNull = (BoolExpr) z3Context.mkOr(leftOperand.getSymbolicNull(), rightOperand.getSymbolicNull()).simplify();
                return new SymbolicColumn(outputValue, outputNull, z3Context);

            }
        }
    }

   static private BoolExpr buildCompareResult(Context z3Context, Expr leftExpr, Expr rightExpr, ComparisonType type){
        ArithExpr leftAExpr = (ArithExpr)leftExpr;
        ArithExpr rightAExpr = (ArithExpr)rightExpr;
        switch (type){
            case LESS_THAN:
                return z3Context.mkLt(leftAExpr, rightAExpr);
            case LESS_THAN_OR_EQUAL_TO:
                return z3Context.mkLe(leftAExpr, rightAExpr);
            case GREATER_THAN:
                return z3Context.mkGt(leftAExpr, rightAExpr);
            case GREATER_THAN_OR_EQUAL_TO:
                return z3Context.mkGe(leftAExpr, rightAExpr);
            case EQUAL_TO:
                return z3Context.mkEq(leftExpr, rightExpr);
            case NOT_EQUAL_TO:
                return z3Context.mkNot(z3Context.mkEq(leftExpr, rightExpr));
            default:{
                //
            }

        }
        return null;
    }
    public static BoolExpr mkAnd(List<BoolExpr> constraints,Context z3Context){
        BoolExpr[] andC = new BoolExpr[constraints.size()];
        constraints.toArray(andC);
        return (BoolExpr) z3Context.mkAnd(andC).simplify();
    }

    static public BoolExpr symbolicColumnEq(SymbolicColumn column1,SymbolicColumn column2,Context z3Context){
        if (trivialEqual(column1,column2)){
            return null;
        }
        BoolExpr bothNull = z3Context.mkAnd(column1.getSymbolicNull(),column2.getSymbolicNull());
        BoolExpr valueEq = z3Context.mkAnd(z3Context.mkEq(column1.getSymbolicNull(),column2.getSymbolicNull()),z3Context.mkEq(column1.getSymbolicValue(),column2.getSymbolicValue()));
        return (BoolExpr) z3Context.mkOr(bothNull,valueEq).simplify();
    }

    static private boolean trivialEqual (SymbolicColumn column1, SymbolicColumn column2){
        if (trivialEqual(column1.getSymbolicValue(),column2.getSymbolicValue())){
            if (trivialEqual(column1.getSymbolicNull(),column2.getSymbolicNull())){
                return true;
            }
        }
        return false;

    }

    static private boolean trivialEqual (Expr e1, Expr e2) {
        if (e1.isTrue() && e2.isTrue()){
            return true;
        }else if (e1.isFalse() && e2.isFalse()){
            return true;
        } else  if (e1.isRatNum() && e2.isRatNum()) {
            RatNum e1V = (RatNum) e1;
            RatNum e2V = (RatNum) e2;
            return e1V.getBigIntNumerator().equals(e2V.getBigIntDenominator());
        } else if (e1.isIntNum() && e2.isIntNum()){
            IntNum e1V = (IntNum) e1;
            IntNum e2V = (IntNum) e2;
            return e1V.getBigInteger().equals(e2V.getBigInteger());
        } else if (e1.isConst() && e2.isConst()){
            return e1.getSExpr().equals(e2.getSExpr());
        } else {
            return false;
        }
    }

}
