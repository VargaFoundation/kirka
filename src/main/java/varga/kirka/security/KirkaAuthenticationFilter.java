package varga.kirka.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.util.matcher.IpAddressMatcher;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

/**
 * Authentication filter for Kirka.
 *
 * <p>Supports three complementary schemes:
 * <ul>
 *   <li>Knox gateway header {@code X-Forwarded-User}, honoured only when the request arrives
 *       from an IP listed in {@code security.trusted.proxies} (CIDR). This prevents clients
 *       that reach the pod directly from impersonating arbitrary users.</li>
 *   <li>HTTP {@code Authorization: Basic} with the password verified against an htpasswd
 *       bcrypt store ({@link HtpasswdUserStore}). A decoded header alone never grants access.</li>
 *   <li>Kerberos principal exposed by the servlet container via {@code request.getUserPrincipal()}
 *       (typical when Knox or an SPNEGO-terminating proxy sits in front).</li>
 * </ul>
 * Admin authority is granted when the authenticated user appears in
 * {@code security.admin.users}; the list is consulted only after successful authentication.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "security.enabled", havingValue = "true")
public class KirkaAuthenticationFilter extends OncePerRequestFilter {

    private static final String KNOX_USER_HEADER = "X-Forwarded-User";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BASIC_PREFIX = "Basic ";

    private final HtpasswdUserStore userStore;

    @Value("${security.authentication.type:basic}")
    private String authenticationType;

    @Value("${security.admin.users:}")
    private String adminUsers;

    @Value("${security.trusted.proxies:}")
    private String trustedProxiesCsv;

    private List<IpAddressMatcher> trustedProxyMatchers = Collections.emptyList();

    public KirkaAuthenticationFilter(HtpasswdUserStore userStore) {
        this.userStore = userStore;
    }

    @Override
    protected void initFilterBean() {
        List<IpAddressMatcher> matchers = new ArrayList<>();
        if (trustedProxiesCsv != null && !trustedProxiesCsv.isBlank()) {
            for (String cidr : trustedProxiesCsv.split(",")) {
                String trimmed = cidr.trim();
                if (trimmed.isEmpty()) continue;
                try {
                    matchers.add(new IpAddressMatcher(trimmed));
                } catch (IllegalArgumentException e) {
                    log.error("Ignoring malformed CIDR in security.trusted.proxies: {}", trimmed);
                }
            }
        }
        this.trustedProxyMatchers = List.copyOf(matchers);
        if (matchers.isEmpty()) {
            log.warn("security.trusted.proxies is empty; the X-Forwarded-User header will be ignored");
        } else {
            log.info("Trusted proxy CIDRs for X-Forwarded-User: {}", trustedProxiesCsv);
        }
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {

        String username = extractUsername(request);

        if (username != null && !username.isEmpty()) {
            List<SimpleGrantedAuthority> authorities = determineAuthorities(username);
            UsernamePasswordAuthenticationToken authentication =
                new UsernamePasswordAuthenticationToken(username, null, authorities);
            authentication.setDetails(new KirkaAuthenticationDetails(request, username));
            SecurityContextHolder.getContext().setAuthentication(authentication);
            log.debug("Authenticated user: {} with authorities: {}", username, authorities);
        } else {
            log.debug("No authentication found in request from {}", request.getRemoteAddr());
        }

        filterChain.doFilter(request, response);
    }

    /**
     * Returns the authenticated username, or {@code null} to defer to the downstream filters
     * (Spring Security will reject unauthenticated access to protected endpoints).
     */
    private String extractUsername(HttpServletRequest request) {
        // 1. Knox header — trusted only when the request comes from a whitelisted proxy IP.
        String knoxUser = request.getHeader(KNOX_USER_HEADER);
        if (knoxUser != null && !knoxUser.isEmpty()) {
            if (isFromTrustedProxy(request)) {
                log.debug("User extracted from Knox header: {}", knoxUser);
                return normalizeUsername(knoxUser);
            }
            log.warn("Refused X-Forwarded-User={} from untrusted remote {}",
                    knoxUser, request.getRemoteAddr());
        }

        // 2. Basic auth — password must match the stored bcrypt hash.
        String authHeader = request.getHeader(AUTHORIZATION_HEADER);
        if (authHeader != null && authHeader.startsWith(BASIC_PREFIX)) {
            String user = verifyBasicCredentials(authHeader);
            if (user != null) return user;
        }

        // 3. Kerberos principal set upstream (SPNEGO / Knox) — already authenticated by container.
        if (request.getUserPrincipal() != null) {
            String principalName = request.getUserPrincipal().getName();
            log.debug("User extracted from request principal: {}", principalName);
            return normalizeUsername(principalName);
        }

        return null;
    }

    private boolean isFromTrustedProxy(HttpServletRequest request) {
        if (trustedProxyMatchers.isEmpty()) return false;
        String remote = request.getRemoteAddr();
        if (remote == null) return false;
        for (IpAddressMatcher matcher : trustedProxyMatchers) {
            if (matcher.matches(remote)) return true;
        }
        return false;
    }

    private String verifyBasicCredentials(String authHeader) {
        try {
            String base64Credentials = authHeader.substring(BASIC_PREFIX.length());
            String credentials = new String(Base64.getDecoder().decode(base64Credentials));
            int colon = credentials.indexOf(':');
            if (colon <= 0) return null;
            String user = credentials.substring(0, colon);
            String password = credentials.substring(colon + 1);
            if (userStore.verify(user, password)) {
                log.debug("User authenticated via Basic auth: {}", user);
                return user;
            }
            log.info("Basic auth failed for user {}", user);
        } catch (IllegalArgumentException e) {
            log.warn("Failed to decode Basic auth header");
        }
        return null;
    }

    /** Strips the realm from a Kerberos principal ({@code user@REALM.COM} → {@code user}). */
    private String normalizeUsername(String username) {
        if (username == null) return null;
        int atIndex = username.indexOf('@');
        return atIndex > 0 ? username.substring(0, atIndex) : username;
    }

    private List<SimpleGrantedAuthority> determineAuthorities(String username) {
        if (adminUsers != null && !adminUsers.isBlank()) {
            for (String admin : adminUsers.split(",")) {
                if (admin.trim().equalsIgnoreCase(username)) {
                    return List.of(
                        new SimpleGrantedAuthority("ROLE_USER"),
                        new SimpleGrantedAuthority("ROLE_ADMIN")
                    );
                }
            }
        }
        return Collections.singletonList(new SimpleGrantedAuthority("ROLE_USER"));
    }
}
