package edu.uci.ics.texera.workflow.common.workflow.raven;

import org.jooq.types.UInteger;

import java.util.*;

public class V2Node {

    List<String> representation;
    Set<Integer> versions;
    String name;

    public V2Node() {
        representation = new ArrayList<>();
        versions = new HashSet<>();
        name = "";
    }

    public V2Node(List<String> columns, UInteger vid) {
        representation = columns;
        versions = new HashSet<>();
        versions.add(vid.intValue());
        name = UUID.randomUUID().toString();
    }

    public boolean isSuperset(List<String> columns) {
        return representation.containsAll(columns);
    }

    public boolean isEqual(List<String> columns) {
        return representation.equals(columns);
    }

    public boolean isSubset(List<String> columns) {
    return columns.containsAll(representation);
    }

    public void addVersionID(UInteger vid) {
        versions.add(vid.intValue());
    }
}
