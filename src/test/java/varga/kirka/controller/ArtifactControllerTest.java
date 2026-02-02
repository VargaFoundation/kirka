package varga.kirka.controller;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import varga.kirka.model.Run;
import varga.kirka.service.ArtifactService;
import varga.kirka.service.RunService;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
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
        Run run = Run.builder().info(varga.kirka.model.RunInfo.builder().runId("run-1").artifactUri("hdfs:///tmp").build()).build();
        when(runService.getRun("run-1")).thenReturn(run);
        
        varga.kirka.model.FileInfo info = varga.kirka.model.FileInfo.builder()
                .path("model.pkl")
                .isDir(false)
                .fileSize(1024L)
                .build();
        when(artifactService.listArtifacts(anyString())).thenReturn(java.util.List.of(info));

        mockMvc.perform(get("/api/2.0/mlflow/artifacts/list")
                .param("run_id", "run-1")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.files[0].path").value("model.pkl"));
    }

    @Test
    public void testDeleteArtifact() throws Exception {
        Run run = Run.builder().info(varga.kirka.model.RunInfo.builder().runId("run-1").artifactUri("hdfs:///tmp").build()).build();
        when(runService.getRun("run-1")).thenReturn(run);

        mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/api/2.0/mlflow/artifacts/delete")
                .content("{\"run_id\": \"run-1\", \"path\": \"model.pkl\"}")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    public void testUploadArtifact() throws Exception {
        Run run = Run.builder().info(varga.kirka.model.RunInfo.builder().runId("run-1").artifactUri("hdfs:///tmp").build()).build();
        when(runService.getRun("run-1")).thenReturn(run);
        doNothing().when(artifactService).uploadArtifact(anyString(), any());

        MockMultipartFile file = new MockMultipartFile(
                "file",
                "model.pkl",
                MediaType.APPLICATION_OCTET_STREAM_VALUE,
                "test content".getBytes()
        );

        mockMvc.perform(multipart("/api/2.0/mlflow/artifacts/upload")
                .file(file)
                .param("run_id", "run-1")
                .param("path", "models"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.path").exists());
    }

    @Test
    public void testDownloadArtifact() throws Exception {
        Run run = Run.builder().info(varga.kirka.model.RunInfo.builder().runId("run-1").artifactUri("hdfs:///tmp").build()).build();
        when(runService.getRun("run-1")).thenReturn(run);
        doNothing().when(artifactService).downloadArtifact(anyString(), any());

        mockMvc.perform(get("/api/2.0/mlflow/artifacts/download")
                .param("run_id", "run-1")
                .param("path", "model.pkl"))
                .andExpect(status().isOk());
    }

    @Test
    public void testListArtifactsRunNotFound() throws Exception {
        when(runService.getRun("nonexistent")).thenReturn(null);

        mockMvc.perform(get("/api/2.0/mlflow/artifacts/list")
                .param("run_id", "nonexistent")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isInternalServerError());
    }
}
