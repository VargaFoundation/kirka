package varga.kirka.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import varga.kirka.model.RegisteredModel;
import varga.kirka.service.ModelRegistryService;

import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ModelRegistryController.class)
public class ModelRegistryControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private ModelRegistryService modelRegistryService;

    @Test
    public void testCreateRegisteredModel() throws Exception {
        RegisteredModel model = RegisteredModel.builder().name("model1").build();
        when(modelRegistryService.getRegisteredModel("model1")).thenReturn(model);

        mockMvc.perform(post("/api/2.0/mlflow/registered-models/create")
                .content("{\"name\": \"model1\"}")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.registered_model.name").value("model1"));
    }

    @Test
    public void testCreateModelVersion() throws Exception {
        varga.kirka.model.ModelVersion version = varga.kirka.model.ModelVersion.builder().name("m1").version("1").build();
        when(modelRegistryService.createModelVersion(anyString(), anyString(), anyString())).thenReturn(version);

        mockMvc.perform(post("/api/2.0/mlflow/model-versions/create")
                .content("{\"name\": \"m1\", \"source\": \"s1\", \"run_id\": \"r1\"}")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.model_version.version").value("1"));
    }

    @Test
    public void testTransitionModelVersionStage() throws Exception {
        varga.kirka.model.ModelVersion version = varga.kirka.model.ModelVersion.builder().name("m1").version("1").currentStage("Production").build();
        when(modelRegistryService.transitionModelVersionStage(anyString(), anyString(), anyString(), anyBoolean())).thenReturn(version);

        mockMvc.perform(post("/api/2.0/mlflow/model-versions/transition-stage")
                .content("{\"name\": \"m1\", \"version\": \"1\", \"stage\": \"Production\"}")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.model_version.current_stage").value("Production"));
    }

    @Test
    public void testSearchRegisteredModels() throws Exception {
        when(modelRegistryService.searchRegisteredModels(any())).thenReturn(List.of());

        mockMvc.perform(get("/api/2.0/mlflow/registered-models/search")
                .param("filter", "name LIKE 'm1%'")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.registered_models").isArray());
    }

    @Test
    public void testListRegisteredModels() throws Exception {
        when(modelRegistryService.listRegisteredModels()).thenReturn(List.of());

        mockMvc.perform(get("/api/2.0/mlflow/registered-models/list")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.registered_models").isArray());
    }
}
