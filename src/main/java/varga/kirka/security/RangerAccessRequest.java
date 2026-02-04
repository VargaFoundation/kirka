package varga.kirka.security;

import lombok.Data;
import java.util.Map;
import java.util.Set;

/**
 * Represents an access request to be evaluated by Ranger.
 * Contains all the information needed to evaluate an access policy.
 */
@Data
public class RangerAccessRequest {
    
    /** User requesting access */
    private String user;
    
    /** User groups */
    private Set<String> userGroups;
    
    /** Resource type (experiment, run, model, gateway, scorer, prompt) */
    private String resourceType;
    
    /** Resource identifier */
    private String resourceId;
    
    /** Resource tags for tag-based authorization */
    private Map<String, String> resourceTags;
    
    /** Requested access type (read, write, delete, admin) */
    private String accessType;
    
    /** Client IP address */
    private String clientIpAddress;
    
    /** Request date/time */
    private long accessTime;
    
    /** Additional context */
    private Map<String, Object> context;

    public RangerAccessRequest() {
        this.accessTime = System.currentTimeMillis();
    }

    /**
     * Creates an access request with basic parameters.
     */
    public static RangerAccessRequest create(String user, String resourceType, 
                                              String resourceId, String accessType) {
        RangerAccessRequest request = new RangerAccessRequest();
        request.setUser(user);
        request.setResourceType(resourceType);
        request.setResourceId(resourceId);
        request.setAccessType(accessType);
        return request;
    }

    /**
     * Creates an access request with tags.
     */
    public static RangerAccessRequest createWithTags(String user, String resourceType,
                                                      String resourceId, String accessType,
                                                      Map<String, String> tags) {
        RangerAccessRequest request = create(user, resourceType, resourceId, accessType);
        request.setResourceTags(tags);
        return request;
    }
}
