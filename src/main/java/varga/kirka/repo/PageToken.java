package varga.kirka.repo;

import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Opaque cursor for HBase row-based pagination. Callers treat the string as a blackbox;
 * the server encodes the last row key of the previous page into a URL-safe Base64 blob and
 * decodes it back to resume the scan on the next call. Using the row key as the cursor means
 * the pagination is stable under concurrent writes: new rows inserted after the cursor will
 * simply appear on the next page, and deletions shrink pages without producing duplicates.
 */
public final class PageToken {

    /** Maximum allowed value for a {@code max_results} parameter. */
    public static final int MAX_PAGE_SIZE = 10_000;
    /** Default page size when {@code max_results} is absent. */
    public static final int DEFAULT_PAGE_SIZE = 1_000;

    private final byte[] lastRow;

    private PageToken(byte[] lastRow) {
        this.lastRow = lastRow;
    }

    public byte[] lastRow() {
        return lastRow;
    }

    /**
     * The next start row for an HBase scan is the byte immediately after {@code lastRow};
     * this helper increments the last byte without having to include the previous row again.
     */
    public byte[] nextStartRow() {
        byte[] next = new byte[lastRow.length + 1];
        System.arraycopy(lastRow, 0, next, 0, lastRow.length);
        next[lastRow.length] = 0;
        return next;
    }

    public String encode() {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(lastRow);
    }

    public static PageToken of(byte[] lastRow) {
        return new PageToken(lastRow);
    }

    public static PageToken ofRow(String lastRow) {
        return new PageToken(Bytes.toBytes(lastRow));
    }

    /** Returns {@code null} for a missing or unreadable token. */
    public static PageToken decode(String token) {
        if (token == null || token.isBlank()) return null;
        try {
            return new PageToken(Base64.getUrlDecoder().decode(token));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid page_token");
        }
    }

    /** Clamps {@code maxResults} into the accepted range. */
    public static int clampPageSize(Integer maxResults) {
        if (maxResults == null || maxResults <= 0) return DEFAULT_PAGE_SIZE;
        return Math.min(maxResults, MAX_PAGE_SIZE);
    }

    /** A UTF-8 preview of the last row key, for logging. */
    public String preview() {
        return new String(lastRow, StandardCharsets.UTF_8);
    }
}
