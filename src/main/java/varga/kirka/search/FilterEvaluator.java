package varga.kirka.search;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Evaluates a list of {@link FilterClause} against an arbitrary domain object via three
 * lookup functions — one for tags, params and metrics, plus a direct attribute accessor. The
 * evaluator is pure: it performs no I/O and holds no state, so it is safe to cache parsed
 * clauses and reuse them for every row.
 *
 * <p>Clauses are ANDed together (MLFlow's grammar has no disjunction). Every clause must
 * evaluate to {@code true} for a row to match; if any clause references a missing key the
 * evaluator reports {@code false} for that clause (missing key excludes the row, which is
 * the MLFlow semantic).
 */
public final class FilterEvaluator<T> {

    private final Function<T, Map<String, String>> tagExtractor;
    private final Function<T, Map<String, String>> paramExtractor;
    private final Function<T, Map<String, Double>> metricExtractor;
    private final AttributeAccessor<T> attributeAccessor;

    public FilterEvaluator(Function<T, Map<String, String>> tagExtractor,
                           Function<T, Map<String, String>> paramExtractor,
                           Function<T, Map<String, Double>> metricExtractor,
                           AttributeAccessor<T> attributeAccessor) {
        this.tagExtractor = tagExtractor;
        this.paramExtractor = paramExtractor;
        this.metricExtractor = metricExtractor;
        this.attributeAccessor = attributeAccessor;
    }

    public boolean matches(T row, List<FilterClause> clauses) {
        if (clauses == null || clauses.isEmpty()) return true;
        for (FilterClause c : clauses) {
            if (!evaluate(row, c)) return false;
        }
        return true;
    }

    private boolean evaluate(T row, FilterClause c) {
        Object left = lookup(row, c);
        if (left == null) return false;
        return switch (c.op()) {
            case EQ -> equalsLoose(left, c.firstValue());
            case NEQ -> !equalsLoose(left, c.firstValue());
            case LT -> compareNumeric(left, c.firstValue()) < 0;
            case LTE -> compareNumeric(left, c.firstValue()) <= 0;
            case GT -> compareNumeric(left, c.firstValue()) > 0;
            case GTE -> compareNumeric(left, c.firstValue()) >= 0;
            case LIKE -> likeMatch(String.valueOf(left), String.valueOf(c.firstValue()), false);
            case ILIKE -> likeMatch(String.valueOf(left), String.valueOf(c.firstValue()), true);
            case IN -> containsLoose(c.values(), left);
            case NOT_IN -> !containsLoose(c.values(), left);
        };
    }

    private Object lookup(T row, FilterClause c) {
        return switch (c.field()) {
            case TAG -> safeGet(tagExtractor.apply(row), c.key());
            case PARAM -> safeGet(paramExtractor.apply(row), c.key());
            case METRIC -> safeGet(metricExtractor.apply(row), c.key());
            case ATTRIBUTE -> attributeAccessor.get(row, c.key());
        };
    }

    private static <V> V safeGet(Map<String, V> map, String key) {
        return map != null ? map.get(key) : null;
    }

    // --- comparisons ------------------------------------------------------------------

    private static boolean equalsLoose(Object left, Object right) {
        if (left == null || right == null) return Objects.equals(left, right);
        if (left instanceof Number || right instanceof Number) {
            try {
                return Double.compare(asDouble(left), asDouble(right)) == 0;
            } catch (NumberFormatException ignored) {
                // fall through to string comparison
            }
        }
        return String.valueOf(left).equals(String.valueOf(right));
    }

    private static int compareNumeric(Object left, Object right) {
        return Double.compare(asDouble(left), asDouble(right));
    }

    private static double asDouble(Object v) {
        if (v instanceof Number n) return n.doubleValue();
        return Double.parseDouble(String.valueOf(v));
    }

    private static boolean containsLoose(Collection<?> haystack, Object needle) {
        for (Object o : haystack) {
            if (equalsLoose(o, needle)) return true;
        }
        return false;
    }

    /** SQL-style LIKE: {@code %} = any sequence, {@code _} = any single character. */
    private static boolean likeMatch(String value, String pattern, boolean ignoreCase) {
        if (value == null || pattern == null) return false;
        StringBuilder regex = new StringBuilder("^");
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            switch (c) {
                case '%' -> regex.append(".*");
                case '_' -> regex.append('.');
                // Regex metacharacters — escape them to preserve literal meaning
                case '.', '(', ')', '[', ']', '{', '}', '\\', '+', '*', '?', '^', '$', '|' ->
                        regex.append('\\').append(c);
                default -> regex.append(c);
            }
        }
        regex.append('$');
        int flags = ignoreCase ? java.util.regex.Pattern.CASE_INSENSITIVE : 0;
        return java.util.regex.Pattern.compile(regex.toString(), flags).matcher(value).matches();
    }

    /** Lookup contract for bare attributes on the domain object. */
    @FunctionalInterface
    public interface AttributeAccessor<T> {
        Object get(T row, String attribute);
    }
}
