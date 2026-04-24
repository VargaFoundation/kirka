package varga.kirka.search;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FilterEvaluatorTest {

    /** A simple row with arbitrary tags/params/metrics/attributes for evaluator testing. */
    private record Row(Map<String, String> tags,
                       Map<String, String> params,
                       Map<String, Double> metrics,
                       Map<String, Object> attributes) {}

    private static final FilterEvaluator<Row> EVALUATOR = new FilterEvaluator<>(
            Row::tags, Row::params, Row::metrics,
            (row, name) -> row.attributes().get(name));

    private static Row row(Map<String, Object> attributes,
                           Map<String, String> tags,
                           Map<String, String> params,
                           Map<String, Double> metrics) {
        return new Row(
                tags != null ? tags : Map.of(),
                params != null ? params : Map.of(),
                metrics != null ? metrics : Map.of(),
                attributes != null ? attributes : Map.of());
    }

    @Test
    void emptyClausesMatchEverything() {
        Row r = row(Map.of("name", "exp1"), null, null, null);
        assertTrue(EVALUATOR.matches(r, List.of()));
    }

    @Test
    void tagEquality() {
        Row r = row(null, Map.of("env", "prod"), null, null);
        assertTrue(EVALUATOR.matches(r, FilterParser.parse("tags.env = 'prod'")));
        assertFalse(EVALUATOR.matches(r, FilterParser.parse("tags.env = 'dev'")));
        // Missing key → clause does not match.
        assertFalse(EVALUATOR.matches(r, FilterParser.parse("tags.missing = 'prod'")));
    }

    @Test
    void metricNumericComparison() {
        Row r = row(null, null, null, Map.of("accuracy", 0.92));
        assertTrue(EVALUATOR.matches(r, FilterParser.parse("metrics.accuracy > 0.9")));
        assertTrue(EVALUATOR.matches(r, FilterParser.parse("metrics.accuracy >= 0.92")));
        assertFalse(EVALUATOR.matches(r, FilterParser.parse("metrics.accuracy > 0.95")));
        assertTrue(EVALUATOR.matches(r, FilterParser.parse("metrics.accuracy != 0.5")));
    }

    @Test
    void paramStringStoredAsNumber() {
        // Clients often log learning rate as a string. The evaluator must still compare it
        // numerically when the right-hand side is a number.
        Row r = row(null, null, Map.of("lr", "0.01"), null);
        assertTrue(EVALUATOR.matches(r, FilterParser.parse("params.lr = '0.01'")));
        // Numeric RHS works too
        assertTrue(EVALUATOR.matches(r, FilterParser.parse("params.lr = 0.01")));
    }

    @Test
    void likeWithPercentAndUnderscore() {
        Row r = row(Map.of("name", "prod_experiment_42"), null, null, null);
        assertTrue(EVALUATOR.matches(r, FilterParser.parse("name LIKE 'prod_%'")));
        // `_` in LIKE matches exactly one char — pattern must align length-wise with the value
        assertTrue(EVALUATOR.matches(r, FilterParser.parse("name LIKE 'prod_experiment___'")));
        // One underscore short: pattern expects 17 chars but name has 18
        assertFalse(EVALUATOR.matches(r, FilterParser.parse("name LIKE 'prod_experiment__'")));
        assertFalse(EVALUATOR.matches(r, FilterParser.parse("name LIKE 'dev_%'")));
    }

    @Test
    void ilikeIsCaseInsensitive() {
        Row r = row(Map.of("name", "ProdExperiment"), null, null, null);
        assertFalse(EVALUATOR.matches(r, FilterParser.parse("name LIKE 'prod%'")));
        assertTrue(EVALUATOR.matches(r, FilterParser.parse("name ILIKE 'prod%'")));
    }

    @Test
    void inAndNotIn() {
        Row r = row(Map.of("status", "FINISHED"), null, null, null);
        assertTrue(EVALUATOR.matches(r,
                FilterParser.parse("status IN ('FINISHED','FAILED')")));
        assertFalse(EVALUATOR.matches(r,
                FilterParser.parse("status IN ('RUNNING','SCHEDULED')")));
        assertTrue(EVALUATOR.matches(r,
                FilterParser.parse("status NOT IN ('RUNNING')")));
    }

    @Test
    void conjunctionEverythingMustMatch() {
        Map<String, Object> attrs = new HashMap<>(Map.of("status", "FINISHED"));
        Row r = row(attrs, Map.of("env", "prod"), Map.of("lr", "0.01"),
                Map.of("accuracy", 0.91));
        assertTrue(EVALUATOR.matches(r, FilterParser.parse(
                "metrics.accuracy > 0.9 AND tags.env = 'prod' AND status = 'FINISHED'")));
        assertFalse(EVALUATOR.matches(r, FilterParser.parse(
                "metrics.accuracy > 0.9 AND tags.env = 'staging'")));
    }

    @Test
    void regexMetacharactersInValueAreEscapedInLike() {
        // Literal '.' in the value must not be treated as regex "any char"
        Row r = row(Map.of("name", "exp.one"), null, null, null);
        assertTrue(EVALUATOR.matches(r, FilterParser.parse("name LIKE 'exp.one'")));
        Row notMatch = row(Map.of("name", "expXone"), null, null, null);
        assertFalse(EVALUATOR.matches(notMatch, FilterParser.parse("name LIKE 'exp.one'")));
    }
}
