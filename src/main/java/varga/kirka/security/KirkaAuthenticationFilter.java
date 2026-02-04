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
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

/**
 * Authentication filter for Kirka.
 * Supports multiple authentication methods:
 * - Knox header (X-Forwarded-User) for authentication via Knox Gateway
 * - Kerberos header (Authorization: Negotiate) 
 * - Basic Auth (Authorization: Basic)
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "security.enabled", havingValue = "true")
public class KirkaAuthenticationFilter extends OncePerRequestFilter {

    private static final String KNOX_USER_HEADER = "X-Forwarded-User";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BASIC_PREFIX = "Basic ";

    @Value("${security.authentication.type:basic}")
    private String authenticationType;

    @Value("${security.admin.users:admin}")
    private String adminUsers;

    @Override
    protected void doFilterInternal(HttpServletRequest request, 
                                    HttpServletResponse response, 
                                    FilterChain filterChain) throws ServletException, IOException {
        
        String username = extractUsername(request);
        
        if (username != null && !username.isEmpty()) {
            List<SimpleGrantedAuthority> authorities = determineAuthorities(username);
            
            UsernamePasswordAuthenticationToken authentication = 
                new UsernamePasswordAuthenticationToken(username, null, authorities);
            
            // Store additional information in details
            authentication.setDetails(new KirkaAuthenticationDetails(request, username));
            
            SecurityContextHolder.getContext().setAuthentication(authentication);
            log.debug("Authenticated user: {} with authorities: {}", username, authorities);
        } else {
            log.debug("No authentication found in request");
        }
        
        filterChain.doFilter(request, response);
    }

    /**
     * Extracts the username from the request based on the configured authentication type.
     */
    private String extractUsername(HttpServletRequest request) {
        // 1. Try Knox header (priority if present)
        String knoxUser = request.getHeader(KNOX_USER_HEADER);
        if (knoxUser != null && !knoxUser.isEmpty()) {
            log.debug("User extracted from Knox header: {}", knoxUser);
            return normalizeUsername(knoxUser);
        }

        // 2. Try Basic authentication
        String authHeader = request.getHeader(AUTHORIZATION_HEADER);
        if (authHeader != null && authHeader.startsWith(BASIC_PREFIX)) {
            try {
                String base64Credentials = authHeader.substring(BASIC_PREFIX.length());
                String credentials = new String(Base64.getDecoder().decode(base64Credentials));
                String[] parts = credentials.split(":", 2);
                if (parts.length == 2) {
                    log.debug("User extracted from Basic auth: {}", parts[0]);
                    return parts[0];
                }
            } catch (Exception e) {
                log.warn("Failed to decode Basic auth header", e);
            }
        }

        // 3. Try Kerberos principal (if available via container)
        if (request.getUserPrincipal() != null) {
            String principalName = request.getUserPrincipal().getName();
            log.debug("User extracted from request principal: {}", principalName);
            return normalizeUsername(principalName);
        }

        return null;
    }

    /**
     * Normalizes the username (removes Kerberos realm if present).
     */
    private String normalizeUsername(String username) {
        if (username == null) return null;
        // Remove Kerberos realm (user@REALM.COM -> user)
        int atIndex = username.indexOf('@');
        if (atIndex > 0) {
            return username.substring(0, atIndex);
        }
        return username;
    }

    /**
     * Determines the authorities (roles) of the user.
     */
    private List<SimpleGrantedAuthority> determineAuthorities(String username) {
        // Check if user is admin
        if (adminUsers != null) {
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
