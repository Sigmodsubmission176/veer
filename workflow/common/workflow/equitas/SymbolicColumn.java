package edu.uci.ics.texera.workflow.common.workflow.equitas;

import com.microsoft.z3.BoolExpr;
import com.microsoft.z3.Context;
import com.microsoft.z3.Expr;
import com.microsoft.z3.Sort;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SymbolicColumn {
    static  private int count=0;
    private Expr symbolicValue;
    private BoolExpr symbolicNull;
    private Context z3Context;
    private AttributeType type;

    public SymbolicColumn(Expr symbolicValue,BoolExpr symbolicNull,Context z3Context){
        this.symbolicValue = symbolicValue;
        this.symbolicNull = symbolicNull;
        this.type = AttributeType.BOOLEAN;
        this.z3Context = z3Context;
    }

    public SymbolicColumn(Expr symbolicValue,BoolExpr symbolicNull,Context z3Context, AttributeType type){
        this.symbolicValue = symbolicValue;
        this.symbolicNull = symbolicNull;
        this.type = type;
        this.z3Context = z3Context;
    }

    static public SymbolicColumn mkNewSymbolicColumn(Context z3Context, AttributeType type){
        Expr value = z3Context.mkConst("value"+count,getSortBasedOnType(z3Context,type));
        BoolExpr valueNull = z3Context.mkBoolConst("isn"+count);
        count++;
        return (new SymbolicColumn(value,valueNull,z3Context, type));
    }

    static public Map<String, SymbolicColumn> constructSymbolicTuple(List<Attribute> attributes, Context z3Context){
        Map<String, SymbolicColumn> result = new HashMap<>();
        for (Attribute attribute : attributes) {
            result.put(attribute.getName(), SymbolicColumn.mkNewSymbolicColumn(z3Context, attribute.getType()));
        }
        return result;
    }

    public AttributeType getType() { return type; }
    public BoolExpr getSymbolicNull(){
        return symbolicNull;
    }

    public Expr getSymbolicValue() {
        return symbolicValue;
    }

    public BoolExpr isValueTrue(){
        return z3Context.mkAnd((BoolExpr)symbolicValue,z3Context.mkNot(symbolicNull));
    }

    static public Sort getSortBasedOnType(Context z3Context, AttributeType type) {
        if (type == AttributeType.ANY) {
            return z3Context.mkIntSort();
        } else if(type == AttributeType.INTEGER) {
            return z3Context.mkIntSort();
        } else if(type == AttributeType.LONG) {
            return z3Context.mkRealSort();
        } else if(type == AttributeType.STRING) {
            return z3Context.mkIntSort();
        } else if(type == AttributeType.TIMESTAMP) {
            return z3Context.mkIntSort();
        } else if(type == AttributeType.DOUBLE) {
            return z3Context.mkRealSort();
        } else if(type == AttributeType.BOOLEAN) {
            return z3Context.mkBoolSort();
        } else if(type == AttributeType.BINARY) {
            return z3Context.mkIntSort();
        } else {
            return z3Context.mkIntSort();
        }
    }

}
