package varga.kirka.security;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helper to access the security context and perform authorization checks.
 * Provides utility methods for controllers and services.
 */
@Slf4j
@Component
public class SecurityContextHelper {

    @Autowired
    private AuthorizationService authorizationService;

    /**
     * Gets the current user.
     * @return the username or "anonymous" if not authenticated
     */
    public String getCurrentUser() {
        return authorizationService.getCurrentUser();
    }

    /**
     * Checks if the current user has read access to a resource.
     */
    public boolean canRead(String resourceType, String resourceId, String owner, Map<String, String> tags) {
        return authorizationService.isAccessAllowed(resourceType, resourceId, owner, tags, 
                AuthorizationService.AccessType.READ);
    }

    /**
     * Checks if the current user has write access to a resource.
     */
    public boolean canWrite(String resourceType, String resourceId, String owner, Map<String, String> tags) {
        return authorizationService.isAccessAllowed(resourceType, resourceId, owner, tags, 
                AuthorizationService.AccessType.WRITE);
    }

    /**
     * Checks if the current user can delete a resource.
     */
    public boolean canDelete(String resourceType, String resourceId, String owner, Map<String, String> tags) {
        return authorizationService.isAccessAllowed(resourceType, resourceId, owner, tags, 
                AuthorizationService.AccessType.DELETE);
    }

    /**
     * Checks if the current user has admin rights on a resource.
     */
    public boolean canAdmin(String resourceType, String resourceId, String owner, Map<String, String> tags) {
        return authorizationService.isAccessAllowed(resourceType, resourceId, owner, tags, 
                AuthorizationService.AccessType.ADMIN);
    }

    /**
     * Checks read access and throws an exception if denied.
     */
    public void checkReadAccess(String resourceType, String resourceId, String owner, Map<String, String> tags) {
        if (!canRead(resourceType, resourceId, owner, tags)) {
            throw new AccessDeniedException(resourceType, resourceId, getCurrentUser(), "read");
        }
    }

    /**
     * Checks write access and throws an exception if denied.
     */
    public void checkWriteAccess(String resourceType, String resourceId, String owner, Map<String, String> tags) {
        if (!canWrite(resourceType, resourceId, owner, tags)) {
            throw new AccessDeniedException(resourceType, resourceId, getCurrentUser(), "write");
        }
    }

    /**
     * Checks delete access and throws an exception if denied.
     */
    public void checkDeleteAccess(String resourceType, String resourceId, String owner, Map<String, String> tags) {
        if (!canDelete(resourceType, resourceId, owner, tags)) {
            throw new AccessDeniedException(resourceType, resourceId, getCurrentUser(), "delete");
        }
    }

    /**
     * Converts a list of tags (ExperimentTag, etc.) to a Map for authorization.
     */
    public <T> Map<String, String> tagsToMap(java.util.List<T> tags, 
                                              java.util.function.Function<T, String> keyExtractor,
                                              java.util.function.Function<T, String> valueExtractor) {
        if (tags == null) {
            return Map.of();
        }
        return tags.stream()
                .collect(Collectors.toMap(keyExtractor, valueExtractor, (v1, v2) -> v1));
    }
}
