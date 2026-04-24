package varga.kirka.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.slf4j.MDC;
import varga.kirka.model.AuditEvent;
import varga.kirka.observability.MdcCorrelationFilter;
import varga.kirka.repo.AuditRepository;
import varga.kirka.repo.Page;
import varga.kirka.security.SecurityContextHelper;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AuditServiceTest {

    @Mock
    private AuditRepository auditRepository;

    @Mock
    private SecurityContextHelper securityContextHelper;

    @InjectMocks
    private AuditService auditService;

    @BeforeEach
    void setUp() {
        when(securityContextHelper.getCurrentUser()).thenReturn("alice");
        MDC.clear();
    }

    @Test
    void recordEnrichesEventWithUserAndRequestId() throws Exception {
        MDC.put(MdcCorrelationFilter.MDC_KEY, "req-123");

        auditService.record("delete", "experiment", "exp-7", "allowed", null);

        ArgumentCaptor<AuditEvent> captor = ArgumentCaptor.forClass(AuditEvent.class);
        verify(auditRepository).append(captor.capture());
        AuditEvent event = captor.getValue();
        assertEquals("delete", event.getAction());
        assertEquals("experiment", event.getResourceType());
        assertEquals("exp-7", event.getResourceId());
        assertEquals("allowed", event.getOutcome());
        assertEquals("alice", event.getUser());
        assertEquals("req-123", event.getRequestId());
        assertNotNull(event.getEventId());
        assertTrue(event.getTimestamp() > 0);
    }

    @Test
    void recordFailsSilentlyWhenHBaseIsDown() throws Exception {
        // The business operation must not be rolled back if the audit write fails.
        doThrow(new IOException("HBase down")).when(auditRepository).append(any());

        assertDoesNotThrow(() ->
                auditService.record("create", "run", "run-1", "allowed", null));
    }

    @Test
    void recordFallsBackToAnonymousWhenNoUser() throws Exception {
        when(securityContextHelper.getCurrentUser()).thenReturn(null);

        auditService.record("create", "experiment", "new-exp", "allowed", null);

        ArgumentCaptor<AuditEvent> captor = ArgumentCaptor.forClass(AuditEvent.class);
        verify(auditRepository).append(captor.capture());
        assertEquals("anonymous", captor.getValue().getUser());
    }

    @Test
    void searchDelegatesToRepository() throws Exception {
        AuditEvent event = AuditEvent.builder().eventId("e1").action("delete").build();
        when(auditRepository.search(anyInt(), any())).thenReturn(new Page<>(List.of(event), "tok"));

        Page<AuditEvent> result = auditService.search(10, null);

        assertEquals(1, result.items().size());
        assertEquals("tok", result.nextPageToken());
    }

    @Test
    void searchReturnsEmptyPageOnRepositoryError() throws Exception {
        when(auditRepository.search(anyInt(), any())).thenThrow(new IOException("boom"));

        Page<AuditEvent> result = auditService.search(10, null);

        assertTrue(result.items().isEmpty());
        assertNull(result.nextPageToken());
    }
}
