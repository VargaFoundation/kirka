package varga.kirka.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import varga.kirka.model.AuditEvent;
import varga.kirka.repo.Page;
import varga.kirka.service.AuditService;

import java.util.List;

/**
 * Admin-facing read endpoint for the Kirka audit log. Listing the audit trail returns the
 * most recent events first, paginated through {@link Page} tokens. Every non-GET mutation is
 * already recorded automatically by {@link varga.kirka.observability.AuditAspect}.
 *
 * <p>Guarded by {@code ROLE_ADMIN} at the method level. When security is disabled
 * (development / test setups where no admin role is mapped) the endpoint is inaccessible by
 * default — this is intentional: the audit trail often contains user identifiers and should
 * not leak through an unauthenticated Spring Boot instance.
 */
@Slf4j
@RestController
@RequestMapping("/api/2.0/kirka/audit")
@RequiredArgsConstructor
public class AuditController {

    private final AuditService auditService;

    @lombok.Data
    @lombok.AllArgsConstructor
    public static class AuditResponse {
        private List<AuditEvent> events;
        private String next_page_token;
    }

    @GetMapping("/search")
    @PreAuthorize("hasRole('ADMIN')")
    public AuditResponse search(@RequestParam(value = "max_results", required = false) Integer maxResults,
                                @RequestParam(value = "page_token", required = false) String pageToken) {
        Page<AuditEvent> page = auditService.search(maxResults, pageToken);
        return new AuditResponse(page.items(), page.nextPageToken());
    }
}
