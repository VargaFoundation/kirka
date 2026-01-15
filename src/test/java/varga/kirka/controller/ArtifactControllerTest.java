package varga.kirka.controller;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import varga.kirka.model.Run;
import varga.kirka.service.ArtifactService;
import varga.kirka.service.RunService;

import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ArtifactController.class)
public class ArtifactControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private ArtifactService artifactService;

    @MockBean
    private RunService runService;

    @Test
    public void testListArtifacts() throws Exception {
        Run run = Run.builder().runId("run-1").artifactUri("hdfs:///tmp").build();
        when(runService.getRun("run-1")).thenReturn(run);
        
        FileStatus status = new FileStatus(100, false, 3, 1024, 0, new Path("hdfs:///tmp/model.pkl"));
        when(artifactService.listArtifacts(anyString())).thenReturn(List.of(status));

        mockMvc.perform(get("/api/2.0/mlflow/artifacts/list")
                .param("run_id", "run-1")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.files[0].path").value("model.pkl"));
    }
}
