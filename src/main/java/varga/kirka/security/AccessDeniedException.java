package varga.kirka.security;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Exception levée quand l'accès à une ressource est refusé.
 */
@ResponseStatus(HttpStatus.FORBIDDEN)
public class AccessDeniedException extends RuntimeException {

    private final String resourceType;
    private final String resourceId;
    private final String user;

    public AccessDeniedException(String message) {
        super(message);
        this.resourceType = null;
        this.resourceId = null;
        this.user = null;
    }

    public AccessDeniedException(String resourceType, String resourceId, String user) {
        super(String.format("Access denied for user '%s' on resource %s/%s", user, resourceType, resourceId));
        this.resourceType = resourceType;
        this.resourceId = resourceId;
        this.user = user;
    }

    public AccessDeniedException(String resourceType, String resourceId, String user, String action) {
        super(String.format("Access denied for user '%s' to %s on resource %s/%s", user, action, resourceType, resourceId));
        this.resourceType = resourceType;
        this.resourceId = resourceId;
        this.user = user;
    }

    public String getResourceType() {
        return resourceType;
    }

    public String getResourceId() {
        return resourceId;
    }

    public String getUser() {
        return user;
    }
}
