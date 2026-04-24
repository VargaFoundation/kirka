package varga.kirka.repo;

import java.util.List;

/**
 * A slice of results plus an optional cursor for the next slice. {@code nextPageToken} is
 * {@code null} when the caller has reached the end of the range.
 */
public record Page<T>(List<T> items, String nextPageToken) {

    public static <T> Page<T> terminal(List<T> items) {
        return new Page<>(items, null);
    }

    public static <T> Page<T> of(List<T> items, String nextPageToken) {
        return new Page<>(items, nextPageToken);
    }
}
