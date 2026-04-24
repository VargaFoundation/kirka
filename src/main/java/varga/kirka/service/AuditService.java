package varga.kirka.service;

import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import varga.kirka.model.AuditEvent;
import varga.kirka.observability.MdcCorrelationFilter;
import varga.kirka.repo.AuditRepository;
import varga.kirka.repo.Page;
import varga.kirka.repo.PageToken;
import varga.kirka.security.SecurityContextHelper;

import java.util.UUID;

/**
 * Thin write+read facade over {@link AuditRepository}. Write-side calls enrich the event
 * with the authenticated user, the remote IP and the MDC request-id; read-side calls are
 * gated at the controller layer (admin-only).
 *
 * <p>Audit failures never break the calling business operation — if HBase is unavailable the
 * event is logged at WARN level and discarded. This is the correct trade-off for a tracking
 * server: blocking a model registration because we couldn't write the audit row would cause
 * more production incidents than it would prevent compliance issues.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AuditService {

    private final AuditRepository auditRepository;
    private final SecurityContextHelper securityContextHelper;

    public void record(String action, String resourceType, String resourceId,
                       String outcome, String reason) {
        AuditEvent event = AuditEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .timestamp(System.currentTimeMillis())
                .user(safeUser())
                .clientIp(safeClientIp())
                .action(action)
                .resourceType(resourceType)
                .resourceId(resourceId)
                .outcome(outcome)
                .reason(reason)
                .requestId(MDC.get(MdcCorrelationFilter.MDC_KEY))
                .build();
        try {
            auditRepository.append(event);
        } catch (Exception e) {
            // Never rollback the caller on audit failure — log and move on.
            log.warn("Failed to persist audit event {} on {}:{} ({}): {}",
                    action, resourceType, resourceId, outcome, e.toString());
        }
    }

    public Page<AuditEvent> search(Integer maxResults, String pageToken) {
        int cap = PageToken.clampPageSize(maxResults);
        PageToken token = PageToken.decode(pageToken);
        try {
            return auditRepository.search(cap, token);
        } catch (Exception e) {
            log.error("Audit search failed: {}", e.toString());
            return Page.terminal(java.util.List.of());
        }
    }

    private String safeUser() {
        try {
            String current = securityContextHelper.getCurrentUser();
            return current != null ? current : "anonymous";
        } catch (Exception e) {
            return "anonymous";
        }
    }

    private String safeClientIp() {
        try {
            ServletRequestAttributes attrs =
                    (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            if (attrs == null) return null;
            HttpServletRequest req = attrs.getRequest();
            // Prefer X-Forwarded-For when behind Knox / a mesh proxy, falling back to the
            // servlet remote address otherwise.
            String xff = req.getHeader("X-Forwarded-For");
            if (xff != null && !xff.isBlank()) {
                int comma = xff.indexOf(',');
                return comma > 0 ? xff.substring(0, comma).trim() : xff.trim();
            }
            return req.getRemoteAddr();
        } catch (Exception e) {
            return null;
        }
    }
}
