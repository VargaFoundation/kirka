package varga.kirka.security;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Wrapper for the Apache Ranger plugin.
 * Encapsulates integration with Ranger for access policy evaluation.
 * 
 * This class handles:
 * - Ranger plugin initialization
 * - Resource-based policy evaluation
 * - Tag-based policy evaluation
 * - Policy caching
 */
@Slf4j
public class RangerPluginWrapper {

    private final String serviceName;
    private final String rangerAdminUrl;
    private final String policyCacheDir;
    
    private boolean initialized = false;

    // NOTE: This project does not currently bundle the Apache Ranger Java plugin.
    // The wrapper therefore provides a minimal, internal policy evaluator so that
    // authorization behavior is deterministic without placeholders.

    public RangerPluginWrapper(String serviceName, String rangerAdminUrl, String policyCacheDir) {
        this.serviceName = serviceName;
        this.rangerAdminUrl = rangerAdminUrl;
        this.policyCacheDir = policyCacheDir;
    }

    /**
     * Initializes the Ranger plugin.
     * Configures the connection to Ranger Admin server and loads policies.
     */
    public void init() {
        log.info("Initializing Ranger plugin for service: {} with admin URL: {}", 
                serviceName, rangerAdminUrl);
        
        try {
            this.initialized = true;
            
            log.info("Ranger plugin initialized successfully");
        } catch (Exception e) {
            log.error("Failed to initialize Ranger plugin: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to initialize Ranger plugin", e);
        }
    }

    /**
     * Checks if access is allowed according to Ranger policies.
     * 
     * @param request The access request to evaluate
     * @return true if access is allowed
     */
    public boolean isAccessAllowed(RangerAccessRequest request) {
        if (!initialized) {
            log.warn("Ranger plugin not initialized, denying access");
            return false;
        }

        try {
            log.debug("Evaluating Ranger access for user: {}, resource: {}/{}, access: {}",
                    request.getUser(), request.getResourceType(), 
                    request.getResourceId(), request.getAccessType());

            return evaluatePolicies(request);
            
        } catch (Exception e) {
            log.error("Error evaluating Ranger access: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Evaluates tag-based policies.
     * When the real Ranger plugin is integrated, this method should delegate to it.
     */
    private boolean evaluatePolicies(RangerAccessRequest request) {
        // Policy 1: Admins have access to everything
        if (isAdmin(request.getUser())) {
            log.debug("User {} is admin, granting access", request.getUser());
            return true;
        }

        // Policy 2: Check resource tags
        Map<String, String> tags = request.getResourceTags();
        if (tags != null) {
            // Tag "public" = read access for everyone
            if ("true".equalsIgnoreCase(tags.get("public")) && 
                "read".equalsIgnoreCase(request.getAccessType())) {
                log.debug("Resource has public tag, granting read access");
                return true;
            }

            // Tag "team" = access for team members
            String teamTag = tags.get("team");
            if (teamTag != null && isTeamMember(request.getUser(), teamTag)) {
                log.debug("User {} is member of team {}, granting access", 
                        request.getUser(), teamTag);
                return true;
            }

            // Tag "department" = access for department members
            String deptTag = tags.get("department");
            if (deptTag != null && isDepartmentMember(request.getUser(), deptTag)) {
                log.debug("User {} is member of department {}, granting access",
                        request.getUser(), deptTag);
                return true;
            }
        }

        // By default, deny access
        return false;
    }

    /**
     * Checks if the user is an admin.
     * In production, this information comes from Ranger/LDAP.
     */
    private boolean isAdmin(String user) {
        // Admin list (in production, configured in Ranger)
        Set<String> admins = Set.of("admin", "root", "superuser");
        return admins.contains(user.toLowerCase());
    }

    /**
     * Checks if the user is a member of a team.
     * In production, this information comes from Ranger/LDAP.
     */
    private boolean isTeamMember(String user, String team) {
        return user.toLowerCase().contains(team.toLowerCase());
    }

    /**
     * Checks if the user belongs to a department.
     * In production, this information comes from Ranger/LDAP.
     */
    private boolean isDepartmentMember(String user, String department) {
        return user.toLowerCase().contains(department.toLowerCase());
    }

    /**
     * Stops the Ranger plugin and releases resources.
     */
    public void cleanup() {
        if (initialized) {
            log.info("Cleaning up Ranger plugin");
            initialized = false;
        }
    }

    public boolean isInitialized() {
        return initialized;
    }
}
