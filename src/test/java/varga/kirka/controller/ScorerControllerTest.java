package varga.kirka.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import varga.kirka.model.Scorer;
import varga.kirka.service.ScorerService;

import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ScorerController.class)
public class ScorerControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private ScorerService scorerService;

    @Test
    public void testRegisterScorer() throws Exception {
        Scorer scorer = Scorer.builder()
                .scorerId("s1")
                .experimentId("1")
                .scorerName("scorer1")
                .scorerVersion(1)
                .serializedScorer("{}")
                .build();

        when(scorerService.registerScorer(anyString(), anyString(), anyString())).thenReturn(scorer);

        mockMvc.perform(post("/api/2.0/mlflow/scorers/register")
                .content("{\"experiment_id\": \"1\", \"name\": \"scorer1\", \"serialized_scorer\": \"{}\"}")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.scorer.scorerId").value("s1"))
                .andExpect(jsonPath("$.scorer.scorerName").value("scorer1"));
    }

    @Test
    public void testListScorers() throws Exception {
        when(scorerService.listScorers("1")).thenReturn(List.of());

        mockMvc.perform(get("/api/2.0/mlflow/scorers/list")
                .param("experiment_id", "1")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.scorers").isArray());
    }

    @Test
    public void testGetScorer() throws Exception {
        Scorer scorer = Scorer.builder().scorerName("scorer1").scorerVersion(1).build();
        when(scorerService.getScorer(eq("1"), eq("scorer1"), any())).thenReturn(scorer);

        mockMvc.perform(get("/api/2.0/mlflow/scorers/get")
                .param("experiment_id", "1")
                .param("name", "scorer1")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.scorer.scorerName").value("scorer1"));
    }

    @Test
    public void testDeleteScorer() throws Exception {
        mockMvc.perform(delete("/api/2.0/mlflow/scorers/delete")
                .param("experiment_id", "e1")
                .param("name", "scorer1")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    public void testListScorerVersions() throws Exception {
        Scorer v1 = Scorer.builder().scorerName("scorer1").scorerVersion(1).build();
        Scorer v2 = Scorer.builder().scorerName("scorer1").scorerVersion(2).build();
        when(scorerService.listScorerVersions("1", "scorer1")).thenReturn(List.of(v1, v2));

        mockMvc.perform(get("/api/2.0/mlflow/scorers/versions")
                .param("experiment_id", "1")
                .param("name", "scorer1")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.scorers").isArray())
                .andExpect(jsonPath("$.scorers.length()").value(2));
    }
}
