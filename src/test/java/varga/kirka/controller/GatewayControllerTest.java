package varga.kirka.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;
import varga.kirka.service.GatewaySecretService;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(GatewayController.class)
public class GatewayControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private GatewaySecretService gatewaySecretService;

    @Test
    public void testAttachModel() throws Exception {
        String requestJson = """
                {
                    "endpoint_id": "test-endpoint",
                    "model_config": {
                        "name": "gpt-4",
                        "provider": "openai"
                    }
                }
                """;

        mockMvc.perform(post("/api/2.0/mlflow/gateway/endpoints/models/attach")
                .content(requestJson)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.mapping.endpoint_id").value("test-endpoint"));
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
        mockMvc.perform(get("/api/2.0/mlflow/gateway/secrets/get")
                .param("secret_name", "test-secret"))
                .andExpect(status().isOk());
    }
}
