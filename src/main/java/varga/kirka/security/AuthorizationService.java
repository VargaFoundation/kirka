package varga.kirka.security;

import java.util.List;
import java.util.Map;

/**
 * Common interface for authorization services.
 * Allows injecting either RangerAuthorizationService (security enabled)
 * or NoOpAuthorizationService (security disabled).
 */
public interface AuthorizationService {

    /**
     * Supported access types.
     */
    enum AccessType {
        READ,
        WRITE,
        DELETE,
        ADMIN
    }

    /**
     * Checks if the current user has access to a resource.
     * 
     * @param resourceType Resource type (experiment, run, model, etc.)
     * @param resourceId Resource identifier
     * @param resourceOwner Owner of the resource
     * @param resourceTags Resource tags
     * @param accessType Requested access type
     * @return true if access is allowed
     */
    boolean isAccessAllowed(String resourceType, String resourceId, 
                           String resourceOwner, Map<String, String> resourceTags,
                           AccessType accessType);

    /**
     * Checks if the current user can create a resource.
     */
    boolean canCreate(String resourceType);

    /**
     * Filters a list of resources based on user permissions.
     */
    <T> List<T> filterAccessibleResources(List<T> resources, String resourceType,
                                          ResourceMetadataExtractor<T> metadataExtractor);

    /**
     * Gets the current user.
     */
    String getCurrentUser();

    /**
     * Interface to extract metadata from a resource.
     */
    @FunctionalInterface
    interface ResourceMetadataExtractor<T> {
        ResourceMetadata extract(T resource);
    }

    /**
     * Resource metadata for authorization.
     */
    record ResourceMetadata(String id, String owner, Map<String, String> tags) {}
}
