package edu.uci.ics.texera.workflow.common.workflow.raven;

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor;

import java.util.ArrayList;
import java.util.List;

public class RecyclerNode {

    int equivalenceClass;
    OperatorDescriptor operator;
    int window;
    String name;
    List<Integer> checkedClasses;

    public RecyclerNode(OperatorDescriptor op, int ec, String nodeName) {
        equivalenceClass = ec;
        checkedClasses = new ArrayList<>();
        operator = op;
        name = nodeName;
    }
}
