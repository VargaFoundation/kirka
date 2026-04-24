package varga.kirka.security;

import jakarta.servlet.FilterChain;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KirkaAuthenticationFilterTest {

    private HtpasswdUserStore userStore;
    private KirkaAuthenticationFilter filter;
    private FilterChain chain;

    @BeforeEach
    void setUp() {
        userStore = mock(HtpasswdUserStore.class);
        chain = mock(FilterChain.class);
        SecurityContextHolder.clearContext();
        filter = new KirkaAuthenticationFilter(userStore);
        ReflectionTestUtils.setField(filter, "authenticationType", "basic");
        ReflectionTestUtils.setField(filter, "adminUsers", "");
        ReflectionTestUtils.setField(filter, "trustedProxiesCsv", "10.0.0.0/24");
        filter.initFilterBean();
    }

    @AfterEach
    void tearDown() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void basicAuthWithWrongPasswordIsRejected() throws Exception {
        when(userStore.verify("alice", "wrong")).thenReturn(false);
        MockHttpServletRequest req = new MockHttpServletRequest();
        req.addHeader("Authorization", basicHeader("alice", "wrong"));
        MockHttpServletResponse resp = new MockHttpServletResponse();

        filter.doFilterInternal(req, resp, chain);

        assertNull(SecurityContextHolder.getContext().getAuthentication(),
                "No authentication should be set when the password check fails");
        verify(chain).doFilter(req, resp);
    }

    @Test
    void basicAuthWithCorrectPasswordAuthenticates() throws Exception {
        when(userStore.verify("alice", "alice-secret")).thenReturn(true);
        MockHttpServletRequest req = new MockHttpServletRequest();
        req.addHeader("Authorization", basicHeader("alice", "alice-secret"));
        MockHttpServletResponse resp = new MockHttpServletResponse();

        filter.doFilterInternal(req, resp, chain);

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        assertNotNull(auth);
        assertEquals("alice", auth.getName());
    }

    @Test
    void knoxHeaderFromTrustedProxyIsHonoured() throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest();
        req.setRemoteAddr("10.0.0.42");
        req.addHeader("X-Forwarded-User", "carol@REALM");
        MockHttpServletResponse resp = new MockHttpServletResponse();

        filter.doFilterInternal(req, resp, chain);

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        assertNotNull(auth);
        assertEquals("carol", auth.getName(), "Kerberos realm should be stripped");
    }

    @Test
    void knoxHeaderFromUntrustedProxyIsIgnored() throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest();
        req.setRemoteAddr("192.168.1.10");
        req.addHeader("X-Forwarded-User", "mallory");
        MockHttpServletResponse resp = new MockHttpServletResponse();

        filter.doFilterInternal(req, resp, chain);

        assertNull(SecurityContextHolder.getContext().getAuthentication(),
                "X-Forwarded-User from an unwhitelisted IP must not authenticate the user");
    }

    @Test
    void adminUsersGetAdminRoleOnlyAfterValidBasicAuth() throws Exception {
        ReflectionTestUtils.setField(filter, "adminUsers", "root,ops");
        filter.initFilterBean();
        when(userStore.verify("root", "root-secret")).thenReturn(true);
        MockHttpServletRequest req = new MockHttpServletRequest();
        req.addHeader("Authorization", basicHeader("root", "root-secret"));
        MockHttpServletResponse resp = new MockHttpServletResponse();

        filter.doFilterInternal(req, resp, chain);

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        assertNotNull(auth);
        assertTrue(auth.getAuthorities().stream()
                .anyMatch(a -> a.getAuthority().equals("ROLE_ADMIN")),
                "Authenticated admin user should carry ROLE_ADMIN");
    }

    @Test
    void adminNameWithoutValidCredentialsIsNotElevated() throws Exception {
        ReflectionTestUtils.setField(filter, "adminUsers", "root");
        filter.initFilterBean();
        when(userStore.verify("root", "anything")).thenReturn(false);
        MockHttpServletRequest req = new MockHttpServletRequest();
        req.addHeader("Authorization", basicHeader("root", "anything"));
        MockHttpServletResponse resp = new MockHttpServletResponse();

        filter.doFilterInternal(req, resp, chain);

        assertNull(SecurityContextHolder.getContext().getAuthentication(),
                "Being named 'root' without a valid password must not grant ROLE_ADMIN");
    }

    private static String basicHeader(String user, String password) {
        String creds = user + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(creds.getBytes());
    }
}
