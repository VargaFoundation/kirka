package varga.kirka.security;

import jakarta.servlet.http.HttpServletRequest;
import lombok.Getter;

/**
 * Custom authentication details for Kirka.
 * Stores additional information about the authenticated user.
 */
@Getter
public class KirkaAuthenticationDetails {

    private final String username;
    private final String remoteAddress;
    private final String sessionId;
    private final String userAgent;

    public KirkaAuthenticationDetails(HttpServletRequest request, String username) {
        this.username = username;
        this.remoteAddress = request.getRemoteAddr();
        this.sessionId = request.getSession(false) != null ? request.getSession().getId() : null;
        this.userAgent = request.getHeader("User-Agent");
    }

    @Override
    public String toString() {
        return "KirkaAuthenticationDetails{" +
                "username='" + username + '\'' +
                ", remoteAddress='" + remoteAddress + '\'' +
                '}';
    }
}
