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
    public void testListRegisteredModels() throws Exception {
        when(modelRegistryService.listRegisteredModels()).thenReturn(List.of());

        mockMvc.perform(get("/api/2.0/mlflow/registered-models/list")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.registered_models").isArray());
    }
}
