package varga.kirka.search;

/**
 * Thrown when a client-supplied search filter string cannot be parsed. The controller layer
 * maps this to a 400 INVALID_PARAMETER_VALUE so the MLFlow client gets an actionable message.
 */
public class FilterParseException extends IllegalArgumentException {

    private final int position;

    public FilterParseException(String message, int position) {
        super(message);
        this.position = position;
    }

    public int getPosition() {
        return position;
    }
}
