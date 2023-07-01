package edu.uci.ics.texera.workflow.common.workflow.raven;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jooq.types.UInteger;
import java.util.*;
import java.util.stream.Collectors;

public class V2Graph {

    class VersionOccurence {
        HashMap<Integer, Integer> versionToCounterMap = new HashMap<>();

        void addVersions(Set<Integer> versions, Integer vid) {
        versions.forEach(version -> {
            if(!version.equals(vid)) {
               int counter = !versionToCounterMap.containsKey(version)? 1 : (versionToCounterMap.get(version)+1);
                versionToCounterMap.put(version, counter);
            }
        });
        }

        List<Integer> getVersions() {
            return versionToCounterMap.entrySet().stream().sorted((v1, v2) -> v2.getValue().
                    compareTo(v1.getValue())).map(entry -> entry.getKey()).collect(Collectors.toList());
        }
    }

    DirectedAcyclicGraph<String, DefaultEdge> v2Graph = new DirectedAcyclicGraph<>(DefaultEdge.class);
    private Map<String, V2Node> nodes = new HashMap<>();
    private List<String> sourceNodes = new ArrayList<>();

    public List<Integer> rankAndUpdate(Set<List<String>> representations, UInteger vid) {
        Set<List<String>> handledReps = new HashSet<>();
        VersionOccurence versions = new VersionOccurence();
        representations.forEach(representation -> {
            for (String source : sourceNodes) {
                if(!handledReps.contains(representation)) {
                    if (checkNode(nodes.get(source), representation, vid, versions, handledReps)) {
                        break;
                    }
                }
            }
            if(!handledReps.contains(representation)) {
                handledReps.add(representation);
                Set<String> parents = new HashSet<>();
                insertNode(parents, getChildren(parents, representation), representation, vid);
            }
        });
        return versions.getVersions();
    }

    private boolean checkNode(V2Node node, List<String> columns, UInteger vid,
                              VersionOccurence versions, Set<List<String>> handledReps) {
        if(node.isEqual(columns)) {
            node.addVersionID(vid);
            versions.addVersions(node.versions, vid.intValue());
            handledReps.add(columns);
            return true;
        }
        if(node.isSuperset(columns)) {
            List<String> children = getChildNodes(node.name);
            for (String child : children) {
                boolean matched = checkNode(nodes.get(child), columns, vid, versions, handledReps);
                //  need to check the return, if already matched then exit, otherwise check other siblings
                if (matched || handledReps.contains(columns)) { //TODO check this
                    return matched;
                }
            }
            if (!handledReps.contains(columns)) {
                Set<String> parents = getParents(columns);
                insertNode(parents, getChildren(parents, columns), columns, vid);
                handledReps.add(columns);
            }
        }
        return false; // the base case of not being equal or subset of node
    }

    private List<String> getChildNodes(String node) {
        return v2Graph.outgoingEdgesOf(node).stream().map(e -> v2Graph.getEdgeTarget(e)).collect(Collectors.toList());
    }

    private Set<String> getParents(List<String> representation) {
        Set<String> parents = new HashSet<>();
        Set<String> visited = new HashSet<>();
        sourceNodes.forEach(source ->
                getParent(null, source, representation, parents, visited));
        return parents;
    }

    private void getParent(String parent, String node, List<String> columns, Set<String> parents, Set<String> visited) {
        visited.add(node);
        if(nodes.get(node).isSuperset(columns)) {
                List<String> children = getChildNodes(node);
                if(children.isEmpty()) {
                    parents.add(node);
                    return;
                }
                children.forEach(child -> {
                    if (!visited.contains(child)) {
                        getParent(node, child, columns, parents, visited);
                    }
                });
        }
        else {
            if(parent!= null) {
                // only if other siblings are not parents
                parents.removeAll(v2Graph.getAncestors(parent));
                // if not one of descendants is already in the list
                Set<String> descendant = v2Graph.getDescendants(parent);
                if(parents.isEmpty() || !parents.stream().anyMatch(p -> descendant.contains(p))) {
                    parents.add(parent);
                }
            }
        }
    }

    private Set<String> getChildren(Set<String> parents, List<String> representation) {
        Set<String> children = new HashSet<>();
        Set<String> copiedParents = new HashSet<>(parents);
        if(parents.isEmpty()) {
            // for each source, test if node is superset then add source to children otherwise add source to parents
            sourceNodes.forEach(source -> {
                if(nodes.get(source).isSubset(representation)) {
                    children.add(source);
                }
                else {
                    copiedParents.add(source);
                }
            });
        }
        copiedParents.forEach(parent -> children.addAll(getChildNodes(parent)));
        children.removeIf(child -> !nodes.get(child).isSubset(representation));
        return children;
    }

    private void insertNode(Set<String> parents, Set<String> children, List<String> representation, UInteger vid) {
        // construct a node
        V2Node newNode = new V2Node(representation, vid);
        nodes.put(newNode.name, newNode);
        // add it as a vertex and to nodes
        v2Graph.addVertex(newNode.name);
        // update graph links
        parents.forEach(parent -> v2Graph.addEdge(parent, newNode.name));
        children.forEach(child -> v2Graph.addEdge(newNode.name, child));
        // remove links from parents to children
        if(!v2Graph.edgeSet().isEmpty() && !parents.isEmpty() && !children.isEmpty())
            parents.forEach(parent -> children.forEach(child ->
                    v2Graph.removeEdge(parent, child)));
        // don't forget to update sources if parents is empty
        if(parents.isEmpty()) {
            sourceNodes.add(newNode.name);
        }
        sourceNodes.removeAll(children);
    }
}
