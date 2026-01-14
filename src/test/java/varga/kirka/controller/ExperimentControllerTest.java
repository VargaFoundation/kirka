package varga.kirka.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import varga.kirka.model.Experiment;
import varga.kirka.service.ExperimentService;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

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
}