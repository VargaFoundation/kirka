package varga.kirka.search;

import java.util.List;

/**
 * One conjunctive clause in an MLFlow search filter, already tokenised and ready to evaluate.
 * <p>Examples:
 * <pre>
 *   tags.env          =   "production"     -> field=TAG, key="env", op=EQ, values=["production"]
 *   metrics.accuracy  &gt;=  0.9               -> field=METRIC, key="accuracy", op=GTE, values=[0.9]
 *   attributes.status IN  ("FINISHED","FAILED") -> field=ATTRIBUTE, key="status", op=IN, values=[...]
 *   name              LIKE "prod%"          -> field=ATTRIBUTE, key="name", op=LIKE, values=["prod%"]
 * </pre>
 */
public record FilterClause(Field field, String key, Operator op, List<Object> values) {

    public enum Field {
        /** {@code tags.<key>} */ TAG,
        /** {@code params.<key>} */ PARAM,
        /** {@code metrics.<key>} */ METRIC,
        /** {@code attributes.<key>} or a bare attribute like {@code name}, {@code status}, … */
        ATTRIBUTE
    }

    public enum Operator {
        EQ, NEQ, LT, LTE, GT, GTE, LIKE, ILIKE, IN, NOT_IN
    }

    /** First (and usually only) value — handy when the operator is scalar. */
    public Object firstValue() {
        return values.isEmpty() ? null : values.get(0);
    }
}
