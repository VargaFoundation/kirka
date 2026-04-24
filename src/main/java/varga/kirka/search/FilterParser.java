package varga.kirka.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Parses an MLFlow-style search filter string into a list of {@link FilterClause}. MLFlow's
 * grammar has only conjunction ({@code AND}); there is no {@code OR} and no nesting. Each
 * clause is {@code <identifier> <operator> <literal-or-list>}, separated by {@code AND}.
 *
 * <p>Grammar implemented:
 * <pre>
 *   filter       := clause ("AND" clause)*
 *   clause       := identifier operator operand
 *   identifier   := ("tags"|"params"|"metrics"|"attributes") "." key
 *                 | bare-attribute          (e.g. name, status, start_time, artifact_uri, …)
 *   operator     := "="|"!="|"&lt;"|"&lt;="|"&gt;"|"&gt;="|"LIKE"|"ILIKE"|"IN"|"NOT IN"
 *   operand      := "'" string "'" | number | "(" operand ("," operand)* ")"
 * </pre>
 *
 * <p>Keys inside brackets (e.g. {@code tags.`team-name`}) and escaped single-quotes inside
 * string literals ({@code 'it''s'}) are both supported. The parser is deliberately permissive
 * about whitespace but strict about quoting — unquoted identifiers after an operator are
 * rejected so stray typos don't silently match a real column.
 *
 * <p>The parser is pure (no state), thread-safe, and raises {@link FilterParseException} with a
 * character offset on any grammatical error.
 */
public final class FilterParser {

    private final String input;
    private int pos;

    private FilterParser(String input) {
        this.input = input;
        this.pos = 0;
    }

    public static List<FilterClause> parse(String filter) {
        if (filter == null || filter.isBlank()) return List.of();
        FilterParser p = new FilterParser(filter);
        List<FilterClause> clauses = new ArrayList<>();
        clauses.add(p.parseClause());
        while (p.skipWhitespaceAndPeek() != -1) {
            String kw = p.readKeyword();
            if (!"AND".equalsIgnoreCase(kw)) {
                throw p.error("Expected 'AND', found '" + kw + "'");
            }
            clauses.add(p.parseClause());
        }
        return clauses;
    }

    // ---- Clause parsing ----------------------------------------------------------------

    private FilterClause parseClause() {
        skipWhitespace();
        // Identifier: `prefix.key` or bare attribute.
        String prefix = readIdentifier();
        String key;
        FilterClause.Field field;
        if (pos < input.length() && input.charAt(pos) == '.') {
            pos++; // consume '.'
            key = readKeyOrBacktickedKey();
            field = switch (prefix.toLowerCase(Locale.ROOT)) {
                case "tags" -> FilterClause.Field.TAG;
                case "params" -> FilterClause.Field.PARAM;
                case "metrics" -> FilterClause.Field.METRIC;
                case "attributes", "attribute" -> FilterClause.Field.ATTRIBUTE;
                default -> throw error("Unknown field prefix '" + prefix + "'");
            };
        } else {
            // Bare attribute: name, status, start_time, etc.
            field = FilterClause.Field.ATTRIBUTE;
            key = prefix;
        }

        skipWhitespace();
        FilterClause.Operator op = readOperator();
        skipWhitespace();
        List<Object> values = readOperand(op);
        return new FilterClause(field, key, op, values);
    }

    // ---- Tokens ------------------------------------------------------------------------

    private String readIdentifier() {
        int start = pos;
        while (pos < input.length()) {
            char c = input.charAt(pos);
            if (Character.isLetterOrDigit(c) || c == '_') pos++;
            else break;
        }
        if (start == pos) throw error("Expected identifier");
        return input.substring(start, pos);
    }

    private String readKeyOrBacktickedKey() {
        if (pos < input.length() && input.charAt(pos) == '`') {
            int end = input.indexOf('`', pos + 1);
            if (end < 0) throw error("Unterminated backtick-quoted key");
            String key = input.substring(pos + 1, end);
            pos = end + 1;
            return key;
        }
        if (pos < input.length() && input.charAt(pos) == '"') {
            int end = input.indexOf('"', pos + 1);
            if (end < 0) throw error("Unterminated double-quoted key");
            String key = input.substring(pos + 1, end);
            pos = end + 1;
            return key;
        }
        return readIdentifier();
    }

    private FilterClause.Operator readOperator() {
        // Two-char ops first
        if (matches("<=")) return FilterClause.Operator.LTE;
        if (matches(">=")) return FilterClause.Operator.GTE;
        if (matches("!=")) return FilterClause.Operator.NEQ;
        if (matches("<>")) return FilterClause.Operator.NEQ;
        if (matches("=")) return FilterClause.Operator.EQ;
        if (matches("<")) return FilterClause.Operator.LT;
        if (matches(">")) return FilterClause.Operator.GT;
        String kw = peekKeyword();
        if (kw != null) {
            switch (kw.toUpperCase(Locale.ROOT)) {
                case "LIKE" -> { advanceKeyword("LIKE"); return FilterClause.Operator.LIKE; }
                case "ILIKE" -> { advanceKeyword("ILIKE"); return FilterClause.Operator.ILIKE; }
                case "IN" -> { advanceKeyword("IN"); return FilterClause.Operator.IN; }
                case "NOT" -> {
                    advanceKeyword("NOT");
                    skipWhitespace();
                    String kw2 = readKeyword();
                    if (!"IN".equalsIgnoreCase(kw2)) {
                        throw error("Expected 'IN' after 'NOT', got '" + kw2 + "'");
                    }
                    return FilterClause.Operator.NOT_IN;
                }
            }
        }
        throw error("Expected a comparison operator");
    }

    private List<Object> readOperand(FilterClause.Operator op) {
        if (op == FilterClause.Operator.IN || op == FilterClause.Operator.NOT_IN) {
            if (!matches("(")) throw error("Expected '(' after IN / NOT IN");
            List<Object> items = new ArrayList<>();
            skipWhitespace();
            if (matches(")")) return items; // empty list → nothing matches, but still valid
            items.add(readScalar());
            skipWhitespace();
            while (matches(",")) {
                skipWhitespace();
                items.add(readScalar());
                skipWhitespace();
            }
            if (!matches(")")) throw error("Expected ')' closing IN list");
            return items;
        }
        return List.of(readScalar());
    }

    private Object readScalar() {
        skipWhitespace();
        if (pos >= input.length()) throw error("Expected a value");
        char c = input.charAt(pos);
        if (c == '\'' || c == '"') return readStringLiteral(c);
        if (c == '-' || Character.isDigit(c)) return readNumber();
        throw error("Expected a quoted string or a number");
    }

    private String readStringLiteral(char quote) {
        pos++; // consume opening quote
        StringBuilder sb = new StringBuilder();
        while (pos < input.length()) {
            char c = input.charAt(pos);
            if (c == quote) {
                // SQL-style escape: two adjacent quotes means a literal quote
                if (pos + 1 < input.length() && input.charAt(pos + 1) == quote) {
                    sb.append(quote);
                    pos += 2;
                    continue;
                }
                pos++; // consume closing quote
                return sb.toString();
            }
            sb.append(c);
            pos++;
        }
        throw error("Unterminated string literal");
    }

    private Number readNumber() {
        int start = pos;
        if (input.charAt(pos) == '-') pos++;
        boolean seenDot = false;
        boolean seenExp = false;
        while (pos < input.length()) {
            char c = input.charAt(pos);
            if (Character.isDigit(c)) { pos++; continue; }
            if (c == '.' && !seenDot && !seenExp) { seenDot = true; pos++; continue; }
            if ((c == 'e' || c == 'E') && !seenExp) { seenExp = true; pos++;
                if (pos < input.length() && (input.charAt(pos) == '+' || input.charAt(pos) == '-')) pos++;
                continue;
            }
            break;
        }
        String raw = input.substring(start, pos);
        try {
            if (seenDot || seenExp) return Double.parseDouble(raw);
            long asLong = Long.parseLong(raw);
            return asLong;
        } catch (NumberFormatException e) {
            throw error("Invalid number '" + raw + "'");
        }
    }

    // ---- Keyword helpers ---------------------------------------------------------------

    /** Returns the next keyword (sequence of letters) without consuming it, or {@code null}. */
    private String peekKeyword() {
        int save = pos;
        skipWhitespace();
        int start = pos;
        while (pos < input.length() && Character.isLetter(input.charAt(pos))) pos++;
        if (start == pos) { pos = save; return null; }
        String kw = input.substring(start, pos);
        pos = save;
        return kw;
    }

    private String readKeyword() {
        skipWhitespace();
        int start = pos;
        while (pos < input.length() && Character.isLetter(input.charAt(pos))) pos++;
        if (start == pos) throw error("Expected a keyword");
        return input.substring(start, pos);
    }

    private void advanceKeyword(String expected) {
        String actual = readKeyword();
        if (!expected.equalsIgnoreCase(actual)) {
            throw error("Expected '" + expected + "', found '" + actual + "'");
        }
    }

    // ---- Whitespace / literal matching -----------------------------------------------

    private boolean matches(String literal) {
        skipWhitespace();
        if (input.regionMatches(pos, literal, 0, literal.length())) {
            pos += literal.length();
            return true;
        }
        return false;
    }

    private void skipWhitespace() {
        while (pos < input.length() && Character.isWhitespace(input.charAt(pos))) pos++;
    }

    private int skipWhitespaceAndPeek() {
        skipWhitespace();
        return pos < input.length() ? input.charAt(pos) : -1;
    }

    private FilterParseException error(String message) {
        return new FilterParseException(message + " at position " + pos
                + " in filter [" + truncate(input) + "]", pos);
    }

    private static String truncate(String s) {
        return s.length() > 120 ? s.substring(0, 117) + "..." : s;
    }

    /** Shallow equality of operators — kept here for tests. */
    static List<FilterClause.Operator> supportedOperators() {
        return Arrays.asList(FilterClause.Operator.values());
    }
}
