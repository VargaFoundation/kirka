package varga.kirka.search;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FilterParserTest {

    @Test
    void parsesEmptyFilter() {
        assertTrue(FilterParser.parse(null).isEmpty());
        assertTrue(FilterParser.parse("").isEmpty());
        assertTrue(FilterParser.parse("   ").isEmpty());
    }

    @Test
    void parsesBareAttributeEquality() {
        List<FilterClause> clauses = FilterParser.parse("name = 'exp1'");
        assertEquals(1, clauses.size());
        FilterClause c = clauses.get(0);
        assertEquals(FilterClause.Field.ATTRIBUTE, c.field());
        assertEquals("name", c.key());
        assertEquals(FilterClause.Operator.EQ, c.op());
        assertEquals("exp1", c.firstValue());
    }

    @Test
    void parsesPrefixedFields() {
        assertEquals(FilterClause.Field.TAG,
                FilterParser.parse("tags.env = 'prod'").get(0).field());
        assertEquals(FilterClause.Field.PARAM,
                FilterParser.parse("params.lr = '0.01'").get(0).field());
        assertEquals(FilterClause.Field.METRIC,
                FilterParser.parse("metrics.accuracy > 0.9").get(0).field());
        assertEquals(FilterClause.Field.ATTRIBUTE,
                FilterParser.parse("attributes.status = 'FINISHED'").get(0).field());
    }

    @Test
    void parsesNumericLiterals() {
        FilterClause c = FilterParser.parse("metrics.acc >= 0.95").get(0);
        assertEquals(FilterClause.Operator.GTE, c.op());
        assertEquals(0.95, ((Number) c.firstValue()).doubleValue(), 1e-9);

        FilterClause step = FilterParser.parse("metrics.step = 100").get(0);
        assertInstanceOf(Long.class, step.firstValue());
        assertEquals(100L, ((Number) step.firstValue()).longValue());
    }

    @Test
    void parsesLikeWithWildcards() {
        FilterClause c = FilterParser.parse("name LIKE 'exp_%'").get(0);
        assertEquals(FilterClause.Operator.LIKE, c.op());
        assertEquals("exp_%", c.firstValue());
    }

    @Test
    void parsesInList() {
        FilterClause c = FilterParser.parse("attributes.status IN ('FINISHED', 'FAILED')").get(0);
        assertEquals(FilterClause.Operator.IN, c.op());
        assertEquals(List.of("FINISHED", "FAILED"), c.values());
    }

    @Test
    void parsesNotIn() {
        FilterClause c = FilterParser.parse("status NOT IN ('RUNNING','SCHEDULED')").get(0);
        assertEquals(FilterClause.Operator.NOT_IN, c.op());
    }

    @Test
    void parsesConjunction() {
        List<FilterClause> clauses = FilterParser.parse(
                "metrics.accuracy > 0.9 AND params.lr = '0.01' AND tags.env = 'prod'");
        assertEquals(3, clauses.size());
        assertEquals(FilterClause.Field.METRIC, clauses.get(0).field());
        assertEquals(FilterClause.Field.PARAM, clauses.get(1).field());
        assertEquals(FilterClause.Field.TAG, clauses.get(2).field());
    }

    @Test
    void parsesBacktickQuotedKeys() {
        FilterClause c = FilterParser.parse("tags.`mlflow.runName` = 'best-run'").get(0);
        assertEquals("mlflow.runName", c.key());
    }

    @Test
    void parsesEscapedQuotesInString() {
        FilterClause c = FilterParser.parse("tags.owner = 'it''s me'").get(0);
        assertEquals("it's me", c.firstValue());
    }

    @Test
    void rejectsInvalidOperator() {
        assertThrows(FilterParseException.class,
                () -> FilterParser.parse("name WRONG 'exp1'"));
    }

    @Test
    void rejectsUnterminatedString() {
        assertThrows(FilterParseException.class,
                () -> FilterParser.parse("name = 'unterminated"));
    }

    @Test
    void rejectsDanglingAnd() {
        assertThrows(FilterParseException.class,
                () -> FilterParser.parse("name = 'x' AND"));
    }

    @Test
    void rejectsUnknownFieldPrefix() {
        assertThrows(FilterParseException.class,
                () -> FilterParser.parse("columns.foo = 'x'"));
    }
}
