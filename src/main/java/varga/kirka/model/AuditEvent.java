package varga.kirka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A single row of the Kirka audit log. Written on every mutating service call (create /
 * update / delete / transition / log*) and consulted by the admin API at
 * {@code /api/2.0/kirka/audit}.
 *
 * <p>The shape mirrors Ranger audit events so that records from both sources can be unified
 * in a downstream SIEM. {@code requestId} matches the MDC value produced by
 * {@link varga.kirka.observability.MdcCorrelationFilter}, which lets ops trace a single user
 * request across logs, metrics and audit trail.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuditEvent {

    /** Opaque event identifier (UUID). Prefer this over the row key when cross-referencing. */
    private String eventId;

    /** Wall-clock time the server recorded the event (epoch millis). */
    private long timestamp;

    /** Authenticated user at the time of the call, or {@code "anonymous"}. */
    private String user;

    /** Remote address of the HTTP client, when available. */
    private String clientIp;

    /** Coarse-grained verb — one of create/update/delete/restore/transition/log/set-tag/delete-tag. */
    private String action;

    /** Resource type targeted by the action (experiment/run/model/model-version/prompt/scorer/gateway-*). */
    private String resourceType;

    /** Resource identifier targeted by the action. */
    private String resourceId;

    /** {@code allowed} / {@code denied} / {@code error}. */
    private String outcome;

    /** Free-form, human-readable description — e.g. the exception message when {@code outcome=error}. */
    private String reason;

    /** Correlation id copied from SLF4J MDC ({@code request_id}). */
    private String requestId;
}
