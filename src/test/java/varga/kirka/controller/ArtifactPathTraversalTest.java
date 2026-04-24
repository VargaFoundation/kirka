package varga.kirka.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import varga.kirka.model.Run;
import varga.kirka.model.RunInfo;
import varga.kirka.service.ArtifactService;
import varga.kirka.service.RunService;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Verifies that ArtifactController rejects path-traversal payloads against the upload,
 * download, list and delete endpoints. The run lookup succeeds in every case so we can
 * confirm that the validator itself is what rejects the request.
 */
@WebMvcTest(ArtifactController.class)
@AutoConfigureMockMvc(addFilters = false)
public class ArtifactPathTraversalTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private ArtifactService artifactService;

    @MockBean
    private RunService runService;

    private static final String[] MALICIOUS_PATHS = {
            "../../etc/passwd",
            "subdir/../../secret",
            "..%2F..%2Fetc%2Fpasswd",  // URL-encoded traversal
            "..%5Cetc%5Cpasswd",       // URL-encoded backslash
            "/etc/passwd",             // absolute
            "\\etc\\passwd",           // windows absolute after normalization
            "model\u0000.pkl",         // embedded null byte
            "a/b/c/..\\..\\escape"     // mixed separators
    };

    @Test
    public void downloadRejectsTraversalPayloads() throws Exception {
        stubRun();
        for (String path : MALICIOUS_PATHS) {
            mockMvc.perform(get("/api/2.0/mlflow/artifacts/download")
                    .param("run_id", "run-1")
                    .param("path", path))
                    .andExpect(status().isBadRequest());
        }
    }

    @Test
    public void listRejectsTraversalPayloads() throws Exception {
        stubRun();
        for (String path : MALICIOUS_PATHS) {
            mockMvc.perform(get("/api/2.0/mlflow/artifacts/list")
                    .param("run_id", "run-1")
                    .param("path", path))
                    .andExpect(status().isBadRequest());
        }
    }

    @Test
    public void deleteRejectsTraversalPayloads() throws Exception {
        stubRun();
        for (String path : MALICIOUS_PATHS) {
            String body = "{\"run_id\": \"run-1\", \"path\": \"" + path.replace("\\", "\\\\").replace("\"", "\\\"") + "\"}";
            mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                    .post("/api/2.0/mlflow/artifacts/delete")
                    .content(body)
                    .contentType(MediaType.APPLICATION_JSON))
                    .andExpect(status().isBadRequest());
        }
    }

    @Test
    public void uploadRejectsTraversalFilename() throws Exception {
        stubRun();
        MockMultipartFile file = new MockMultipartFile(
                "file",
                "../../pwn.sh",
                MediaType.APPLICATION_OCTET_STREAM_VALUE,
                "evil".getBytes());
        mockMvc.perform(multipart("/api/2.0/mlflow/artifacts/upload")
                .file(file)
                .param("run_id", "run-1"))
                .andExpect(status().isBadRequest());
    }

    @Test
    public void legitimateRelativePathIsAccepted() throws Exception {
        stubRun();
        // A benign subpath must still pass the validator (and only be rejected further down if
        // the service rejects it). A 2xx response means validatePath didn't block the request.
        mockMvc.perform(get("/api/2.0/mlflow/artifacts/list")
                .param("run_id", "run-1")
                .param("path", "models/v1"))
                .andExpect(status().is2xxSuccessful());
    }

    private void stubRun() throws java.io.IOException {
        Run run = Run.builder()
                .info(RunInfo.builder().runId("run-1").artifactUri("hdfs:///tmp/run-1").build())
                .build();
        when(runService.getRun("run-1")).thenReturn(run);
    }
}
