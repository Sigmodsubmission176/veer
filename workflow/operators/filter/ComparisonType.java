package edu.uci.ics.texera.workflow.operators.filter;

import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.kudu.client.KuduPredicate;

enum PredicateType {
    /** A predicate which filters all rows. */
    NONE,
    /** A predicate which filters all rows not equal to a value. */
    EQUALITY,
    /** A predicate which filters all rows not in a range. */
    RANGE,
    /** A predicate which filters all null rows. */
    IS_NOT_NULL,
    /** A predicate which filters all non-null rows. */
    IS_NULL,
    /** A predicate which filters all rows not matching a list of values. */
    IN_LIST,
    }

public enum ComparisonType {
    EQUAL_TO("="),

    GREATER_THAN(">"),

    GREATER_THAN_OR_EQUAL_TO(">="),

    LESS_THAN("<"),

    LESS_THAN_OR_EQUAL_TO("<="),

    NOT_EQUAL_TO("!="),

    IS_NULL("is null"),

    IS_NOT_NULL("is not null");

    private final String name;

    private ComparisonType(String name) {
        this.name = name;
    }

    // use the name string instead of enum string in JSON
    @JsonValue
    public String getName() {
        return this.name;
    }

    protected KuduPredicate.ComparisonOp mapToKuduComp() {
        switch (this){
            case EQUAL_TO:
              return  KuduPredicate.ComparisonOp.EQUAL;
            case GREATER_THAN:
                return KuduPredicate.ComparisonOp.GREATER;
            case LESS_THAN:
                return KuduPredicate.ComparisonOp.LESS;
            case GREATER_THAN_OR_EQUAL_TO:
                return KuduPredicate.ComparisonOp.GREATER_EQUAL;
            case LESS_THAN_OR_EQUAL_TO:
                return KuduPredicate.ComparisonOp.LESS_EQUAL;
        }
        return null; //todo should map null and not null and not equals
    }
}
