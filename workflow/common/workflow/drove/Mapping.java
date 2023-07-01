package edu.uci.ics.texera.workflow.common.workflow.drove;

import java.util.Objects;

public class Mapping {
    String op1;
    String op2;

    public Mapping(String op1, String op2) {
        this.op1 = op1;
        this.op2 = op2;
    }

    public String getOp1() {
        return op1;
    }

    public void setOp1(String op1) {
        this.op1 = op1;
    }

    public String getOp2() {
        return op2;
    }

    public void setOp2(String op2) {
        this.op2 = op2;
    }

    @Override
    public int hashCode() {
        return Objects.hash(op1, op2);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Mapping that = (Mapping) obj;
        return that.op1 == op1 && that.op2 == op2;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
