package varga.kirka.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;
import varga.kirka.model.GatewayEndpointModelMapping;
import varga.kirka.model.GatewayRoute;
import varga.kirka.model.GatewaySecretInfo;
import varga.kirka.service.GatewayEndpointService;
import varga.kirka.service.GatewayRouteService;
import varga.kirka.service.GatewaySecretService;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(GatewayController.class)
@AutoConfigureMockMvc(addFilters = false)
public class GatewayControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private GatewaySecretService gatewaySecretService;

    @MockBean
    private GatewayRouteService gatewayRouteService;

    @MockBean
    private GatewayEndpointService gatewayEndpointService;

    @Test
    public void testAttachModel() throws Exception {
        GatewayEndpointModelMapping mapping = GatewayEndpointModelMapping.builder()
                .mappingId("mapping-123")
                .endpointId("test-endpoint")
                .createdAt(System.currentTimeMillis())
                .build();
        when(gatewayEndpointService.attachModel(anyString(), any(), any())).thenReturn(mapping);

        String requestJson = """
                {
                    "endpoint_id": "test-endpoint",
                    "model_definition_id": "model-123",
                    "weight": 1.0
                }
                """;

        mockMvc.perform(post("/api/2.0/mlflow/gateway/endpoints/models/attach")
                .content(requestJson)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.mapping.endpointId").value("test-endpoint"));
    }

    @Test
    public void testDetachModel() throws Exception {
        String requestJson = """
                {
                    "endpoint_id": "test-endpoint",
                    "model_definition_id": "test-model-id"
                }
                """;

        mockMvc.perform(post("/api/2.0/mlflow/gateway/endpoints/models/detach")
                .content(requestJson)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    public void testSetTag() throws Exception {
        String requestJson = """
                {
                    "endpoint_id": "test-endpoint",
                    "key": "test-key",
                    "value": "test-value"
                }
                """;

        mockMvc.perform(post("/api/2.0/mlflow/gateway/endpoints/set-tag")
                .content(requestJson)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    public void testDeleteTag() throws Exception {
        mockMvc.perform(delete("/api/2.0/mlflow/gateway/endpoints/delete-tag")
                .param("endpoint_id", "test-endpoint")
                .param("key", "test-key"))
                .andExpect(status().isOk());
    }

    @Test
    public void testCreateRoute() throws Exception {
        GatewayRoute route = GatewayRoute.builder().name("test-route").build();
        when(gatewayRouteService.createRoute(any())).thenReturn(route);

        String requestJson = """
                {
                    "name": "test-route"
                }
                """;
        mockMvc.perform(post("/api/2.0/mlflow/gateway/routes")
                .content(requestJson)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    public void testCreateSecret() throws Exception {
        GatewaySecretInfo secretInfo = GatewaySecretInfo.builder()
                .secretId("secret-123")
                .secretName("test-secret")
                .provider("openai")
                .build();
        when(gatewaySecretService.createSecret(anyString(), any(), anyString(), any(), anyString())).thenReturn(secretInfo);

        String requestJson = """
                {
                    "secret_name": "test-secret",
                    "secret_value": [{"key": "api_key", "value": "sk-123"}],
                    "provider": "openai",
                    "created_by": "user1"
                }
                """;
        mockMvc.perform(post("/api/2.0/mlflow/gateway/secrets/create")
                .content(requestJson)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    public void testListSecrets() throws Exception {
        mockMvc.perform(get("/api/2.0/mlflow/gateway/secrets/list"))
                .andExpect(status().isOk());
    }

    @Test
    public void testGetSecret() throws Exception {
        GatewaySecretInfo secretInfo = GatewaySecretInfo.builder()
                .secretId("secret-123")
                .secretName("test-secret")
                .build();
        when(gatewaySecretService.getSecret(any(), anyString())).thenReturn(secretInfo);

        mockMvc.perform(get("/api/2.0/mlflow/gateway/secrets/get")
                .param("secret_name", "test-secret"))
                .andExpect(status().isOk());
    }

    @Test
    public void testUpdateSecret() throws Exception {
        GatewaySecretInfo secretInfo = GatewaySecretInfo.builder()
                .secretId("secret-123")
                .secretName("test-secret")
                .build();
        when(gatewaySecretService.updateSecret(anyString(), any(), any(), anyString())).thenReturn(secretInfo);

        String requestJson = """
                {
                    "secret_id": "secret-123",
                    "secret_value": [{"key": "api_key", "value": "sk-456"}],
                    "updated_by": "user2"
                }
                """;
        mockMvc.perform(post("/api/2.0/mlflow/gateway/secrets/update")
                .content(requestJson)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    public void testDeleteSecret() throws Exception {
        String requestJson = """
                {
                    "secret_id": "secret-123"
                }
                """;
        mockMvc.perform(delete("/api/2.0/mlflow/gateway/secrets/delete")
                .content(requestJson)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    public void testListRoutes() throws Exception {
        when(gatewayRouteService.listRoutes()).thenReturn(List.of());

        mockMvc.perform(get("/api/2.0/mlflow/gateway/routes"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.routes").isArray());
    }

    @Test
    public void testGetRoute() throws Exception {
        mockMvc.perform(get("/api/2.0/mlflow/gateway/routes/test-route"))
                .andExpect(status().isOk());
    }

    @Test
    public void testQueryRoute() throws Exception {
        String requestJson = """
                {
                    "prompt": "Hello, how are you?"
                }
                """;
        mockMvc.perform(post("/api/2.0/mlflow/gateway/query/test-route")
                .content(requestJson)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.candidates").isArray());
    }

    @Test
    public void testUpdateRoute() throws Exception {
        String requestJson = """
                {
                    "route_config": {
                        "model": "gpt-4"
                    }
                }
                """;
        mockMvc.perform(patch("/api/2.0/mlflow/gateway/routes/test-route")
                .content(requestJson)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    public void testInvocations() throws Exception {
        String requestJson = """
                {
                    "inputs": [[1, 2, 3]]
                }
                """;
        mockMvc.perform(post("/api/invocations")
                .content(requestJson)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.predictions").isArray());
    }

    @Test
    public void testPing() throws Exception {
        mockMvc.perform(get("/api/ping"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("healthy"));
    }

    @Test
    public void testVersion() throws Exception {
        mockMvc.perform(get("/api/version"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.version").value("1.0.0"));
    }

    @Test
    public void testMetadata() throws Exception {
        mockMvc.perform(get("/api/metadata"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.model_name").value("kirka"));
    }
}
