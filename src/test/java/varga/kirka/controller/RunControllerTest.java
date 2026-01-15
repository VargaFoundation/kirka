package varga.kirka.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import varga.kirka.model.Run;
import varga.kirka.service.RunService;

import java.util.Map;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(RunController.class)
public class RunControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private RunService runService;

    @Test
    public void testCreateRun() throws Exception {
        Run run = Run.builder().runId("run-1").experimentId("exp-1").build();
        when(runService.createRun(anyString(), anyString(), anyLong(), anyMap())).thenReturn(run);

        mockMvc.perform(post("/api/2.0/mlflow/runs/create")
                .content("{\"experiment_id\": \"exp-1\", \"user_id\": \"user-1\"}")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.run.runId").value("run-1"));
    }

    @Test
    public void testGetRun() throws Exception {
        Run run = Run.builder().runId("run-1").experimentId("exp-1").build();
        when(runService.getRun("run-1")).thenReturn(run);

        mockMvc.perform(get("/api/2.0/mlflow/runs/get")
                .param("run_id", "run-1")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.run.runId").value("run-1"));
    }

    @Test
    public void testLogMetric() throws Exception {
        mockMvc.perform(post("/api/2.0/mlflow/runs/log-metric")
                .content("{\"run_id\": \"run-1\", \"key\": \"acc\", \"value\": 0.95}")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }
}
