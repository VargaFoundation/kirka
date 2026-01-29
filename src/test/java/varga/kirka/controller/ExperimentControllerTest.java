package varga.kirka.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import varga.kirka.model.Experiment;
import varga.kirka.service.ExperimentService;

import static org.mockito.ArgumentMatchers.*;

@WebMvcTest(ExperimentController.class)
public class ExperimentControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private ExperimentService experimentService;

    @Test
    public void testGetExperiment() throws Exception {
        Experiment experiment = Experiment.builder().experimentId("123").name("test").build();
        when(experimentService.getExperiment(anyString())).thenReturn(experiment);
        
        mockMvc.perform(get("/api/2.0/mlflow/experiments/get")
                .param("experiment_id", "123")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.experiment.experimentId").value("123"));
    }

    @Test
    public void testCreateExperiment() throws Exception {
        when(experimentService.createExperiment(anyString(), any(), any())).thenReturn("exp-1");
        
        mockMvc.perform(post("/api/2.0/mlflow/experiments/create")
                .content("{\"name\": \"test-exp\", \"artifact_location\": \"hdfs:///tmp\"}")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.experiment_id").value("exp-1"));
    }

    @Test
    public void testUpdateExperiment() throws Exception {
        mockMvc.perform(post("/api/2.0/mlflow/experiments/update")
                .content("{\"experiment_id\": \"123\", \"new_name\": \"new-name\"}")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    public void testDeleteExperiment() throws Exception {
        mockMvc.perform(post("/api/2.0/mlflow/experiments/delete")
                .content("{\"experiment_id\": \"123\"}")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    public void testSearchExperiments() throws Exception {
        when(experimentService.searchExperiments(any(), any(), any())).thenReturn(java.util.List.of());

        mockMvc.perform(get("/api/2.0/mlflow/experiments/search")
                .param("view_type", "ACTIVE_ONLY")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.experiments").isArray());
    }

    @Test
    public void testListExperiments() throws Exception {
        when(experimentService.listExperiments()).thenReturn(java.util.List.of());
        
        mockMvc.perform(get("/api/2.0/mlflow/experiments/list")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.experiments").isArray());
    }
}