package varga.kirka.repo;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Repository;
import varga.kirka.model.AuditEvent;
import varga.kirka.util.HBaseResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Append-only store for {@link AuditEvent}s in the {@code mlflow_audit} HBase table.
 *
 * <p>The row key encodes {@code reversedTimestamp + "_" + eventId}. Because HBase scans are
 * lexicographically ordered, a plain scan returns events from newest to oldest without a
 * sort step on the server. The reversed timestamp is left-padded to 19 digits so rows sort
 * consistently for any positive epoch-millis value.
 *
 * <p>The repository never exposes the raw row key — callers manipulate {@link varga.kirka.repo.PageToken}.
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class AuditRepository {

    private static final String TABLE_NAME = "mlflow_audit";
    private static final byte[] CF_INFO = Bytes.toBytes("info");

    private static final byte[] COL_EVENT_ID = Bytes.toBytes("event_id");
    private static final byte[] COL_TIMESTAMP = Bytes.toBytes("timestamp");
    private static final byte[] COL_USER = Bytes.toBytes("user");
    private static final byte[] COL_CLIENT_IP = Bytes.toBytes("client_ip");
    private static final byte[] COL_ACTION = Bytes.toBytes("action");
    private static final byte[] COL_RESOURCE_TYPE = Bytes.toBytes("resource_type");
    private static final byte[] COL_RESOURCE_ID = Bytes.toBytes("resource_id");
    private static final byte[] COL_OUTCOME = Bytes.toBytes("outcome");
    private static final byte[] COL_REASON = Bytes.toBytes("reason");
    private static final byte[] COL_REQUEST_ID = Bytes.toBytes("request_id");

    private final Connection connection;

    public void append(AuditEvent event) throws IOException {
        if (event == null) return;
        long ts = event.getTimestamp() > 0 ? event.getTimestamp() : System.currentTimeMillis();
        String eventId = event.getEventId() != null ? event.getEventId() : UUID.randomUUID().toString();
        byte[] rowKey = buildRowKey(ts, eventId);

        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Put put = new Put(rowKey);
            put.addColumn(CF_INFO, COL_EVENT_ID, Bytes.toBytes(eventId));
            put.addColumn(CF_INFO, COL_TIMESTAMP, Bytes.toBytes(ts));
            if (event.getUser() != null) put.addColumn(CF_INFO, COL_USER, Bytes.toBytes(event.getUser()));
            if (event.getClientIp() != null) put.addColumn(CF_INFO, COL_CLIENT_IP, Bytes.toBytes(event.getClientIp()));
            if (event.getAction() != null) put.addColumn(CF_INFO, COL_ACTION, Bytes.toBytes(event.getAction()));
            if (event.getResourceType() != null) put.addColumn(CF_INFO, COL_RESOURCE_TYPE, Bytes.toBytes(event.getResourceType()));
            if (event.getResourceId() != null) put.addColumn(CF_INFO, COL_RESOURCE_ID, Bytes.toBytes(event.getResourceId()));
            if (event.getOutcome() != null) put.addColumn(CF_INFO, COL_OUTCOME, Bytes.toBytes(event.getOutcome()));
            if (event.getReason() != null) put.addColumn(CF_INFO, COL_REASON, Bytes.toBytes(event.getReason()));
            if (event.getRequestId() != null) put.addColumn(CF_INFO, COL_REQUEST_ID, Bytes.toBytes(event.getRequestId()));
            table.put(put);
        }
    }

    /**
     * Scans the audit log in reverse-chronological order (newest first). {@code pageToken} is
     * the row key returned from the previous page; pass {@code null} to start at the most
     * recent event.
     */
    public Page<AuditEvent> search(int maxResults, PageToken pageToken) throws IOException {
        List<AuditEvent> events = new ArrayList<>();
        byte[] lastRow = null;

        Scan scan = new Scan();
        scan.setCaching(Math.min(maxResults, 500));
        if (pageToken != null) {
            scan.withStartRow(pageToken.nextStartRow(), true);
        }
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
             ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {
                if (events.size() >= maxResults) break;
                events.add(mapResultToEvent(result));
                lastRow = result.getRow();
            }
        }
        String next = (events.size() == maxResults && lastRow != null) ? PageToken.of(lastRow).encode() : null;
        return new Page<>(events, next);
    }

    private AuditEvent mapResultToEvent(Result r) {
        return AuditEvent.builder()
                .eventId(HBaseResults.getStringOrNull(r, CF_INFO, COL_EVENT_ID))
                .timestamp(HBaseResults.getLongOrDefault(r, CF_INFO, COL_TIMESTAMP, 0L))
                .user(HBaseResults.getStringOrNull(r, CF_INFO, COL_USER))
                .clientIp(HBaseResults.getStringOrNull(r, CF_INFO, COL_CLIENT_IP))
                .action(HBaseResults.getStringOrNull(r, CF_INFO, COL_ACTION))
                .resourceType(HBaseResults.getStringOrNull(r, CF_INFO, COL_RESOURCE_TYPE))
                .resourceId(HBaseResults.getStringOrNull(r, CF_INFO, COL_RESOURCE_ID))
                .outcome(HBaseResults.getStringOrNull(r, CF_INFO, COL_OUTCOME))
                .reason(HBaseResults.getStringOrNull(r, CF_INFO, COL_REASON))
                .requestId(HBaseResults.getStringOrNull(r, CF_INFO, COL_REQUEST_ID))
                .build();
    }

    private static byte[] buildRowKey(long timestampMillis, String eventId) {
        String reversed = String.format("%019d", Long.MAX_VALUE - timestampMillis);
        return Bytes.toBytes(reversed + "_" + eventId);
    }
}
