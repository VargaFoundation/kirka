package varga.kirka.repo;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import varga.kirka.model.GatewayRoute;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties = {
    "spring.main.allow-bean-definition-overriding=true",
    "security.kerberos.enabled=false"
})
@Import(AbstractHBaseIntegrationTest.HBaseTestConfig.class)
public class GatewayRouteRepositoryIntegrationTest extends AbstractHBaseIntegrationTest {

    @Autowired
    private GatewayRouteRepository gatewayRouteRepository;

    @Test
    public void testSaveAndGetRoute() throws IOException {
        GatewayRoute route = GatewayRoute.builder()
                .name("test-route")
                .routeType("llm/v1/completions")
                .modelName("gpt-4")
                .modelProvider("openai")
                .config(Map.of("max_tokens", 1024, "temperature", 0.7))
                .build();

        gatewayRouteRepository.saveRoute(route);

        GatewayRoute retrieved = gatewayRouteRepository.getRouteByName("test-route");
        assertNotNull(retrieved);
        assertEquals("test-route", retrieved.getName());
        assertEquals("llm/v1/completions", retrieved.getRouteType());
        assertEquals("gpt-4", retrieved.getModelName());
        assertEquals("openai", retrieved.getModelProvider());
        assertNotNull(retrieved.getConfig());
        assertEquals(1024, retrieved.getConfig().get("max_tokens"));
    }

    @Test
    public void testGetRouteNotFound() throws IOException {
        GatewayRoute retrieved = gatewayRouteRepository.getRouteByName("nonexistent-route");
        assertNull(retrieved);
    }

    @Test
    public void testListRoutes() throws IOException {
        gatewayRouteRepository.saveRoute(GatewayRoute.builder()
                .name("list-route-1")
                .routeType("llm/v1/completions")
                .modelName("model-a")
                .modelProvider("openai")
                .build());
        gatewayRouteRepository.saveRoute(GatewayRoute.builder()
                .name("list-route-2")
                .routeType("llm/v1/embeddings")
                .modelName("model-b")
                .modelProvider("anthropic")
                .build());

        List<GatewayRoute> routes = gatewayRouteRepository.listRoutes();
        assertNotNull(routes);
        assertTrue(routes.stream().anyMatch(r -> "list-route-1".equals(r.getName())));
        assertTrue(routes.stream().anyMatch(r -> "list-route-2".equals(r.getName())));
    }

    @Test
    public void testUpdateRoute() throws IOException {
        gatewayRouteRepository.saveRoute(GatewayRoute.builder()
                .name("update-route")
                .routeType("llm/v1/completions")
                .modelName("old-model")
                .modelProvider("openai")
                .build());

        // Update by saving with same name (row key)
        gatewayRouteRepository.saveRoute(GatewayRoute.builder()
                .name("update-route")
                .routeType("llm/v1/completions")
                .modelName("new-model")
                .modelProvider("anthropic")
                .build());

        GatewayRoute retrieved = gatewayRouteRepository.getRouteByName("update-route");
        assertEquals("new-model", retrieved.getModelName());
        assertEquals("anthropic", retrieved.getModelProvider());
    }

    @Test
    public void testDeleteRoute() throws IOException {
        gatewayRouteRepository.saveRoute(GatewayRoute.builder()
                .name("delete-route")
                .routeType("llm/v1/completions")
                .modelName("model-x")
                .build());

        assertNotNull(gatewayRouteRepository.getRouteByName("delete-route"));

        gatewayRouteRepository.deleteRoute("delete-route");

        assertNull(gatewayRouteRepository.getRouteByName("delete-route"));
    }

    @Test
    public void testRouteWithNullOptionalFields() throws IOException {
        GatewayRoute route = GatewayRoute.builder()
                .name("minimal-route")
                .build();

        gatewayRouteRepository.saveRoute(route);

        GatewayRoute retrieved = gatewayRouteRepository.getRouteByName("minimal-route");
        assertNotNull(retrieved);
        assertEquals("minimal-route", retrieved.getName());
        assertNull(retrieved.getRouteType());
        assertNull(retrieved.getModelName());
        assertNull(retrieved.getModelProvider());
        assertNull(retrieved.getConfig());
    }
}
