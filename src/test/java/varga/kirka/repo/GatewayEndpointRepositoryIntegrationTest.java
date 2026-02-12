package varga.kirka.repo;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import varga.kirka.model.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties = {
    "spring.main.allow-bean-definition-overriding=true",
    "security.kerberos.enabled=false"
})
@Import(AbstractHBaseIntegrationTest.HBaseTestConfig.class)
public class GatewayEndpointRepositoryIntegrationTest extends AbstractHBaseIntegrationTest {

    @Autowired
    private GatewayEndpointRepository gatewayEndpointRepository;

    @Test
    public void testSaveAndGetEndpoint() throws IOException {
        GatewayEndpoint endpoint = GatewayEndpoint.builder()
                .endpointId("ep-1")
                .name("test-endpoint")
                .createdAt(System.currentTimeMillis())
                .lastUpdatedAt(System.currentTimeMillis())
                .createdBy("admin")
                .lastUpdatedBy("admin")
                .modelMappings(new ArrayList<>())
                .tags(List.of(GatewayEndpointTag.builder().key("env").value("prod").build()))
                .build();

        gatewayEndpointRepository.saveEndpoint(endpoint);

        GatewayEndpoint retrieved = gatewayEndpointRepository.getEndpointById("ep-1");
        assertNotNull(retrieved);
        assertEquals("ep-1", retrieved.getEndpointId());
        assertEquals("test-endpoint", retrieved.getName());
        assertEquals("admin", retrieved.getCreatedBy());
        assertNotNull(retrieved.getTags());
        assertEquals(1, retrieved.getTags().size());
        assertEquals("env", retrieved.getTags().get(0).getKey());
        assertEquals("prod", retrieved.getTags().get(0).getValue());
    }

    @Test
    public void testGetEndpointNotFound() throws IOException {
        assertNull(gatewayEndpointRepository.getEndpointById("nonexistent-ep"));
    }

    @Test
    public void testEndpointWithModelMappings() throws IOException {
        GatewayEndpointModelMapping mapping = GatewayEndpointModelMapping.builder()
                .mappingId("map-1")
                .endpointId("ep-mappings")
                .modelDefinitionId("model-def-1")
                .weight(1.0f)
                .linkageType(GatewayModelLinkageType.PRIMARY)
                .fallbackOrder(0)
                .createdAt(System.currentTimeMillis())
                .createdBy("admin")
                .build();

        GatewayEndpoint endpoint = GatewayEndpoint.builder()
                .endpointId("ep-mappings")
                .name("mapped-endpoint")
                .createdAt(System.currentTimeMillis())
                .lastUpdatedAt(System.currentTimeMillis())
                .createdBy("admin")
                .modelMappings(List.of(mapping))
                .tags(new ArrayList<>())
                .build();

        gatewayEndpointRepository.saveEndpoint(endpoint);

        GatewayEndpoint retrieved = gatewayEndpointRepository.getEndpointById("ep-mappings");
        assertNotNull(retrieved);
        assertNotNull(retrieved.getModelMappings());
        assertEquals(1, retrieved.getModelMappings().size());
        assertEquals("map-1", retrieved.getModelMappings().get(0).getMappingId());
        assertEquals("model-def-1", retrieved.getModelMappings().get(0).getModelDefinitionId());
        assertEquals(1.0f, retrieved.getModelMappings().get(0).getWeight());
        assertEquals(GatewayModelLinkageType.PRIMARY, retrieved.getModelMappings().get(0).getLinkageType());
    }

    @Test
    public void testListEndpoints() throws IOException {
        gatewayEndpointRepository.saveEndpoint(GatewayEndpoint.builder()
                .endpointId("list-ep-1")
                .name("endpoint-1")
                .createdAt(System.currentTimeMillis())
                .lastUpdatedAt(System.currentTimeMillis())
                .build());
        gatewayEndpointRepository.saveEndpoint(GatewayEndpoint.builder()
                .endpointId("list-ep-2")
                .name("endpoint-2")
                .createdAt(System.currentTimeMillis())
                .lastUpdatedAt(System.currentTimeMillis())
                .build());

        List<GatewayEndpoint> endpoints = gatewayEndpointRepository.listEndpoints();
        assertNotNull(endpoints);
        assertTrue(endpoints.stream().anyMatch(e -> "list-ep-1".equals(e.getEndpointId())));
        assertTrue(endpoints.stream().anyMatch(e -> "list-ep-2".equals(e.getEndpointId())));
    }

    @Test
    public void testDeleteEndpoint() throws IOException {
        gatewayEndpointRepository.saveEndpoint(GatewayEndpoint.builder()
                .endpointId("delete-ep")
                .name("to-delete")
                .createdAt(System.currentTimeMillis())
                .lastUpdatedAt(System.currentTimeMillis())
                .build());

        assertNotNull(gatewayEndpointRepository.getEndpointById("delete-ep"));

        gatewayEndpointRepository.deleteEndpoint("delete-ep");

        assertNull(gatewayEndpointRepository.getEndpointById("delete-ep"));
    }

    @Test
    public void testUpdateEndpointAddMapping() throws IOException {
        GatewayEndpoint endpoint = GatewayEndpoint.builder()
                .endpointId("ep-update-map")
                .name("update-endpoint")
                .createdAt(System.currentTimeMillis())
                .lastUpdatedAt(System.currentTimeMillis())
                .modelMappings(new ArrayList<>())
                .tags(new ArrayList<>())
                .build();
        gatewayEndpointRepository.saveEndpoint(endpoint);

        // Add a mapping
        GatewayEndpointModelMapping mapping = GatewayEndpointModelMapping.builder()
                .mappingId("new-map")
                .endpointId("ep-update-map")
                .modelDefinitionId("model-1")
                .weight(0.5f)
                .build();

        endpoint.setModelMappings(List.of(mapping));
        endpoint.setLastUpdatedAt(System.currentTimeMillis());
        gatewayEndpointRepository.saveEndpoint(endpoint);

        GatewayEndpoint retrieved = gatewayEndpointRepository.getEndpointById("ep-update-map");
        assertNotNull(retrieved.getModelMappings());
        assertEquals(1, retrieved.getModelMappings().size());
        assertEquals("new-map", retrieved.getModelMappings().get(0).getMappingId());
    }

    @Test
    public void testEndpointWithRoutingStrategy() throws IOException {
        GatewayEndpoint endpoint = GatewayEndpoint.builder()
                .endpointId("ep-strategy")
                .name("strategy-endpoint")
                .createdAt(System.currentTimeMillis())
                .lastUpdatedAt(System.currentTimeMillis())
                .routingStrategy(RoutingStrategy.REQUEST_BASED_TRAFFIC_SPLIT)
                .build();

        gatewayEndpointRepository.saveEndpoint(endpoint);

        GatewayEndpoint retrieved = gatewayEndpointRepository.getEndpointById("ep-strategy");
        assertNotNull(retrieved);
        assertNotNull(retrieved.getRoutingStrategy());
        assertEquals(RoutingStrategy.REQUEST_BASED_TRAFFIC_SPLIT, retrieved.getRoutingStrategy());
    }
}
