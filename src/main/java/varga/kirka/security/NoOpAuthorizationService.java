package varga.kirka.security;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * No-op (no operation) authorization service.
 * Used when security is disabled (security.enabled=false).
 * Allows all operations without verification.
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "security.enabled", havingValue = "false", matchIfMissing = true)
public class NoOpAuthorizationService implements AuthorizationService {

    public NoOpAuthorizationService() {
        log.info("Security is DISABLED - NoOpAuthorizationService is active, all operations are permitted");
    }

    /**
     * Always allows access (security disabled).
     */
    @Override
    public boolean isAccessAllowed(String resourceType, String resourceId, 
                                   String resourceOwner, Map<String, String> resourceTags,
                                   AccessType accessType) {
        return true;
    }

    /**
     * Always allows creation (security disabled).
     */
    @Override
    public boolean canCreate(String resourceType) {
        return true;
    }

    /**
     * Returns all resources without filtering (security disabled).
     */
    @Override
    public <T> List<T> filterAccessibleResources(List<T> resources, String resourceType,
                                                  ResourceMetadataExtractor<T> metadataExtractor) {
        return resources;
    }

    /**
     * Returns "anonymous" as current user (security disabled).
     */
    @Override
    public String getCurrentUser() {
        return "anonymous";
    }
}
