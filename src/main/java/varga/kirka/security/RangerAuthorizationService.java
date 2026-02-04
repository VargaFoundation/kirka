package varga.kirka.security;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Authorization service integrating Apache Ranger.
 * Checks permissions based on:
 * - Resource owner
 * - Resource tags
 * 
 * This service uses the Ranger API to evaluate access policies.
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "security.enabled", havingValue = "true")
public class RangerAuthorizationService implements AuthorizationService {

    @Value("${ranger.service.name:kirka}")
    private String rangerServiceName;

    @Value("${ranger.admin.url:http://localhost:6080}")
    private String rangerAdminUrl;

    @Value("${ranger.policy.cache.dir:/tmp/ranger-cache}")
    private String policyCacheDir;

    @Value("${security.authorization.owner.enabled:true}")
    private boolean ownerAuthorizationEnabled;

    private RangerPluginWrapper rangerPlugin;

    /**
     * Initializes the Ranger plugin.
     * Called at startup if Ranger is configured.
     */
    @jakarta.annotation.PostConstruct
    public void init() {
        log.info("Initializing Ranger authorization service for service: {}", rangerServiceName);
        try {
            rangerPlugin = new RangerPluginWrapper(rangerServiceName, rangerAdminUrl, policyCacheDir);
            rangerPlugin.init();
            log.info("Ranger plugin initialized successfully");
        } catch (Exception e) {
            log.warn("Failed to initialize Ranger plugin, falling back to owner-based authorization only: {}", 
                    e.getMessage());
            rangerPlugin = null;
        }
    }

    /**
     * Checks if the current user has access to a resource.
     * 
     * @param resourceType Resource type (experiment, run, model, etc.)
     * @param resourceId Resource identifier
     * @param resourceOwner Owner of the resource
     * @param resourceTags Resource tags
     * @param accessType Requested access type (read, write, delete, admin)
     * @return true if access is allowed
     */
    public boolean isAccessAllowed(String resourceType, String resourceId, 
                                   String resourceOwner, Map<String, String> resourceTags,
                                   AccessType accessType) {
        
        String currentUser = getCurrentUser();
        if (currentUser == null) {
            log.debug("No authenticated user, denying access");
            return false;
        }

        // 1. Owner-based verification
        if (ownerAuthorizationEnabled && isOwner(currentUser, resourceOwner)) {
            log.debug("User {} is owner of resource {}/{}, granting access", 
                    currentUser, resourceType, resourceId);
            return true;
        }

        // 2. Ranger verification (if available)
        if (rangerPlugin != null) {
            boolean rangerResult = checkRangerAccess(currentUser, resourceType, resourceId, 
                    resourceTags, accessType);
            log.debug("Ranger authorization result for user {} on {}/{}: {}", 
                    currentUser, resourceType, resourceId, rangerResult);
            return rangerResult;
        }

        // 3. If Ranger is not available and user is not owner, deny access
        log.debug("Access denied for user {} on resource {}/{}", currentUser, resourceType, resourceId);
        return false;
    }

    /**
     * Checks if the current user can create a resource.
     * By default, all authenticated users can create resources.
     */
    public boolean canCreate(String resourceType) {
        String currentUser = getCurrentUser();
        if (currentUser == null) {
            return false;
        }

        if (rangerPlugin != null) {
            return checkRangerAccess(currentUser, resourceType, "*", null, AccessType.WRITE);
        }

        // By default, allow creation for authenticated users
        return true;
    }

    /**
     * Filters a list of resources based on user permissions.
     * 
     * @param resources List of resources with their metadata
     * @param resourceType Resource type
     * @return Filtered list of accessible resources
     */
    public <T> List<T> filterAccessibleResources(List<T> resources, String resourceType,
                                                  ResourceMetadataExtractor<T> metadataExtractor) {
        String currentUser = getCurrentUser();
        if (currentUser == null) {
            return List.of();
        }

        return resources.stream()
                .filter(resource -> {
                    ResourceMetadata metadata = metadataExtractor.extract(resource);
                    return isAccessAllowed(resourceType, metadata.id(), 
                            metadata.owner(), metadata.tags(), AccessType.READ);
                })
                .toList();
    }

    /**
     * Gets the current user from the security context.
     */
    public String getCurrentUser() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.isAuthenticated() && !"anonymousUser".equals(auth.getPrincipal())) {
            return auth.getName();
        }
        return null;
    }

    /**
     * Checks if the user is the owner of the resource.
     */
    private boolean isOwner(String user, String owner) {
        if (owner == null || owner.isEmpty()) {
            return false;
        }
        return user.equalsIgnoreCase(owner);
    }

    /**
     * Checks access via the Ranger plugin.
     */
    private boolean checkRangerAccess(String user, String resourceType, String resourceId,
                                      Map<String, String> resourceTags, AccessType accessType) {
        if (rangerPlugin == null) {
            return false;
        }

        try {
            RangerAccessRequest request = new RangerAccessRequest();
            request.setUser(user);
            request.setResourceType(resourceType);
            request.setResourceId(resourceId);
            request.setResourceTags(resourceTags);
            request.setAccessType(accessType.name().toLowerCase());

            return rangerPlugin.isAccessAllowed(request);
        } catch (Exception e) {
            log.error("Error checking Ranger access: {}", e.getMessage(), e);
            return false;
        }
    }

}
