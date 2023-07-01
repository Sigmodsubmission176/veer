package edu.uci.ics.texera.workflow.common.workflow.drove;

import java.util.Comparator;

public class Segment implements Comparable<Segment>{
    int numberOfOperators;
    int numberOfChanges;
    Decomposition initialDecomposition;

    public Segment(Decomposition decomposition) {
        initialDecomposition = decomposition;
        numberOfChanges = (int) decomposition.windows.stream().filter(window -> window.covering == true).count();
        numberOfOperators = decomposition.windows.size();

    }


    public int getNumberOfOperators() {
        return numberOfOperators;
    }

    public int getNumberOfChanges() {
        return numberOfChanges;
    }

    @Override
    public int compareTo(Segment o) {
        return Comparator.comparing(Segment::getNumberOfChanges).thenComparing(Segment::getNumberOfOperators).compare(this, o); //TODO check if this is correct
    }
}
