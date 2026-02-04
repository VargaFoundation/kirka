package varga.kirka.security;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests unitaires pour les services d'autorisation.
 */
class AuthorizationServiceTest {

    private NoOpAuthorizationService noOpService;

    @BeforeEach
    void setUp() {
        noOpService = new NoOpAuthorizationService();
    }

    @Test
    void testNoOpAuthorizationService_alwaysAllowsAccess() {
        // Given
        String resourceType = "experiment";
        String resourceId = "exp-123";
        String owner = "user1";
        Map<String, String> tags = Map.of("team", "data-science");

        // When/Then - all access types should be allowed
        assertTrue(noOpService.isAccessAllowed(resourceType, resourceId, owner, tags, 
                AuthorizationService.AccessType.READ));
        assertTrue(noOpService.isAccessAllowed(resourceType, resourceId, owner, tags, 
                AuthorizationService.AccessType.WRITE));
        assertTrue(noOpService.isAccessAllowed(resourceType, resourceId, owner, tags, 
                AuthorizationService.AccessType.DELETE));
        assertTrue(noOpService.isAccessAllowed(resourceType, resourceId, owner, tags, 
                AuthorizationService.AccessType.ADMIN));
    }

    @Test
    void testNoOpAuthorizationService_alwaysAllowsCreate() {
        assertTrue(noOpService.canCreate("experiment"));
        assertTrue(noOpService.canCreate("model"));
        assertTrue(noOpService.canCreate("scorer"));
    }

    @Test
    void testNoOpAuthorizationService_returnsAnonymousUser() {
        assertEquals("anonymous", noOpService.getCurrentUser());
    }

    @Test
    void testNoOpAuthorizationService_doesNotFilterResources() {
        // Given
        List<String> resources = List.of("res1", "res2", "res3");

        // When
        List<String> filtered = noOpService.filterAccessibleResources(
                resources, 
                "experiment",
                res -> new AuthorizationService.ResourceMetadata(res, "owner", Map.of())
        );

        // Then - all resources should be returned
        assertEquals(3, filtered.size());
        assertEquals(resources, filtered);
    }

    @Test
    void testAccessType_values() {
        AuthorizationService.AccessType[] types = AuthorizationService.AccessType.values();
        assertEquals(4, types.length);
        assertNotNull(AuthorizationService.AccessType.valueOf("READ"));
        assertNotNull(AuthorizationService.AccessType.valueOf("WRITE"));
        assertNotNull(AuthorizationService.AccessType.valueOf("DELETE"));
        assertNotNull(AuthorizationService.AccessType.valueOf("ADMIN"));
    }

    @Test
    void testResourceMetadata_record() {
        // Given
        String id = "exp-123";
        String owner = "user1";
        Map<String, String> tags = Map.of("team", "data-science", "public", "true");

        // When
        AuthorizationService.ResourceMetadata metadata = 
                new AuthorizationService.ResourceMetadata(id, owner, tags);

        // Then
        assertEquals(id, metadata.id());
        assertEquals(owner, metadata.owner());
        assertEquals(tags, metadata.tags());
    }

    @Test
    void testRangerAccessRequest_creation() {
        // Given
        String user = "testuser";
        String resourceType = "experiment";
        String resourceId = "exp-123";
        String accessType = "read";

        // When
        RangerAccessRequest request = RangerAccessRequest.create(user, resourceType, resourceId, accessType);

        // Then
        assertEquals(user, request.getUser());
        assertEquals(resourceType, request.getResourceType());
        assertEquals(resourceId, request.getResourceId());
        assertEquals(accessType, request.getAccessType());
        assertTrue(request.getAccessTime() > 0);
    }

    @Test
    void testRangerAccessRequest_withTags() {
        // Given
        String user = "testuser";
        String resourceType = "experiment";
        String resourceId = "exp-123";
        String accessType = "read";
        Map<String, String> tags = Map.of("team", "data-science");

        // When
        RangerAccessRequest request = RangerAccessRequest.createWithTags(
                user, resourceType, resourceId, accessType, tags);

        // Then
        assertEquals(user, request.getUser());
        assertEquals(tags, request.getResourceTags());
    }

    @Test
    void testRangerPluginWrapper_initialization() {
        // Given
        RangerPluginWrapper wrapper = new RangerPluginWrapper("kirka", "http://localhost:6080", "/tmp/cache");

        // When
        wrapper.init();

        // Then
        assertTrue(wrapper.isInitialized());
    }

    @Test
    void testRangerPluginWrapper_adminAccess() {
        // Given
        RangerPluginWrapper wrapper = new RangerPluginWrapper("kirka", "http://localhost:6080", "/tmp/cache");
        wrapper.init();

        RangerAccessRequest adminRequest = RangerAccessRequest.create("admin", "experiment", "exp-123", "read");

        // When
        boolean result = wrapper.isAccessAllowed(adminRequest);

        // Then - admin should have access
        assertTrue(result);
    }

    @Test
    void testRangerPluginWrapper_publicTagAccess() {
        // Given
        RangerPluginWrapper wrapper = new RangerPluginWrapper("kirka", "http://localhost:6080", "/tmp/cache");
        wrapper.init();

        RangerAccessRequest request = RangerAccessRequest.createWithTags(
                "regularuser", "experiment", "exp-123", "read", 
                Map.of("public", "true"));

        // When
        boolean result = wrapper.isAccessAllowed(request);

        // Then - public resources should be readable
        assertTrue(result);
    }

    @Test
    void testRangerPluginWrapper_deniesAccessWithoutPermission() {
        // Given
        RangerPluginWrapper wrapper = new RangerPluginWrapper("kirka", "http://localhost:6080", "/tmp/cache");
        wrapper.init();

        RangerAccessRequest request = RangerAccessRequest.create(
                "regularuser", "experiment", "exp-123", "read");

        // When
        boolean result = wrapper.isAccessAllowed(request);

        // Then - regular user without tags should be denied
        assertFalse(result);
    }

    @Test
    void testAccessDeniedException_withMessage() {
        // Given/When
        AccessDeniedException ex = new AccessDeniedException("Access denied");

        // Then
        assertEquals("Access denied", ex.getMessage());
        assertNull(ex.getResourceType());
        assertNull(ex.getResourceId());
        assertNull(ex.getUser());
    }

    @Test
    void testAccessDeniedException_withDetails() {
        // Given/When
        AccessDeniedException ex = new AccessDeniedException("experiment", "exp-123", "user1");

        // Then
        assertTrue(ex.getMessage().contains("user1"));
        assertTrue(ex.getMessage().contains("experiment"));
        assertTrue(ex.getMessage().contains("exp-123"));
        assertEquals("experiment", ex.getResourceType());
        assertEquals("exp-123", ex.getResourceId());
        assertEquals("user1", ex.getUser());
    }

    @Test
    void testAccessDeniedException_withAction() {
        // Given/When
        AccessDeniedException ex = new AccessDeniedException("experiment", "exp-123", "user1", "delete");

        // Then
        assertTrue(ex.getMessage().contains("delete"));
        assertTrue(ex.getMessage().contains("user1"));
    }
}
