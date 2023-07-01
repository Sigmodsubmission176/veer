package edu.uci.ics.texera.workflow.common.workflow.drove;

import java.util.Set;

public interface EV {

    Set<String> getEquivalentSinks(WorkflowDag previousDag, WorkflowDag currentDag);

    boolean isValid(Window window);
}
