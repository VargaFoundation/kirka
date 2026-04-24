package varga.kirka.observability;

import jakarta.servlet.FilterChain;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MdcCorrelationFilterTest {

    private final MdcCorrelationFilter filter = new MdcCorrelationFilter();

    @Test
    void generatesUuidWhenHeaderMissing() throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest();
        MockHttpServletResponse resp = new MockHttpServletResponse();
        AtomicReference<String> captured = new AtomicReference<>();
        FilterChain chain = (r, s) -> captured.set(MDC.get(MdcCorrelationFilter.MDC_KEY));

        filter.doFilter(req, resp, chain);

        String id = captured.get();
        assertNotNull(id);
        assertEquals(36, id.length(), "UUIDs are 36 characters");
        assertEquals(id, resp.getHeader(MdcCorrelationFilter.REQUEST_ID_HEADER));
        assertNull(MDC.get(MdcCorrelationFilter.MDC_KEY), "MDC must be cleared after the request");
    }

    @Test
    void reusesClientSuppliedHeader() throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest();
        req.addHeader(MdcCorrelationFilter.REQUEST_ID_HEADER, "caller-abc-123");
        MockHttpServletResponse resp = new MockHttpServletResponse();
        AtomicReference<String> captured = new AtomicReference<>();
        FilterChain chain = (r, s) -> captured.set(MDC.get(MdcCorrelationFilter.MDC_KEY));

        filter.doFilter(req, resp, chain);

        assertEquals("caller-abc-123", captured.get());
        assertEquals("caller-abc-123", resp.getHeader(MdcCorrelationFilter.REQUEST_ID_HEADER));
    }

    @Test
    void rejectsOversizedHeaderAndGeneratesFresh() throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest();
        // 129 characters – over the 128 cap. Likely garbage from a buggy caller or an attacker
        // trying to bloat log lines; replace it with a fresh UUID.
        req.addHeader(MdcCorrelationFilter.REQUEST_ID_HEADER, "x".repeat(129));
        MockHttpServletResponse resp = new MockHttpServletResponse();
        AtomicReference<String> captured = new AtomicReference<>();
        FilterChain chain = (r, s) -> captured.set(MDC.get(MdcCorrelationFilter.MDC_KEY));

        filter.doFilter(req, resp, chain);

        assertNotEquals("x".repeat(129), captured.get());
        assertEquals(36, captured.get().length());
    }
}
