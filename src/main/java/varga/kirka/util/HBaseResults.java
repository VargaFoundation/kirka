package varga.kirka.util;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Null-safe decoders for HBase Result cells. Prevents NullPointerException when a column
 * was never written (or was deleted) and a primitive decoder (Bytes.toLong, Bytes.toDouble)
 * would blow up on a null byte array.
 */
public final class HBaseResults {

    private HBaseResults() {}

    public static long getLongOrDefault(Result result, byte[] family, byte[] qualifier, long fallback) {
        byte[] bytes = result.getValue(family, qualifier);
        if (bytes == null || bytes.length != Bytes.SIZEOF_LONG) {
            return fallback;
        }
        return Bytes.toLong(bytes);
    }

    public static double getDoubleOrDefault(Result result, byte[] family, byte[] qualifier, double fallback) {
        byte[] bytes = result.getValue(family, qualifier);
        if (bytes == null || bytes.length != Bytes.SIZEOF_DOUBLE) {
            return fallback;
        }
        return Bytes.toDouble(bytes);
    }

    public static String getStringOrNull(Result result, byte[] family, byte[] qualifier) {
        byte[] bytes = result.getValue(family, qualifier);
        return bytes != null ? Bytes.toString(bytes) : null;
    }

    public static String getStringOrDefault(Result result, byte[] family, byte[] qualifier, String fallback) {
        String value = getStringOrNull(result, family, qualifier);
        return value != null ? value : fallback;
    }
}
