package varga.kirka.controller;
import lombok.extern.slf4j.Slf4j;
import varga.kirka.model.FileInfo;
import varga.kirka.model.Run;
import varga.kirka.service.ArtifactService;
import varga.kirka.service.RunService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api/2.0/mlflow/artifacts")
public class ArtifactController {

    @Autowired
    private ArtifactService artifactService;

    @Autowired
    private RunService runService;

    @GetMapping("/list")
    public Map<String, List<FileInfo>> listArtifacts(@RequestParam(value = "run_id", required = true) String runId,
                                            @RequestParam(value = "path", required = false) String path) throws IOException {
        log.debug("REST request to list artifacts for run: {}, path: {}", runId, path);
        Run run = runService.getRun(runId);
        if (run == null) {
            throw new IllegalArgumentException("Run not found: " + runId);
        }
        
        String baseUri = run.getInfo().getArtifactUri();
        String fullPath = baseUri;
        if (path != null && !path.isEmpty()) {
            fullPath = baseUri + (baseUri.endsWith("/") ? "" : "/") + path;
        }
        
        List<FileInfo> files = artifactService.listArtifacts(fullPath);
        
        return Map.of("files", files);
    }

    @PostMapping("/upload")
    public Map<String, String> uploadArtifact(@RequestParam("run_id") String runId,
                                             @RequestParam(value = "path", required = false) String path,
                                             @RequestParam("file") MultipartFile file) throws IOException {
        Run run = runService.getRun(runId);
        if (run == null) {
            throw new IllegalArgumentException("Run not found: " + runId);
        }

        String baseUri = run.getInfo().getArtifactUri();
        String fullPath = baseUri;
        if (path != null && !path.isEmpty()) {
            fullPath = baseUri + (baseUri.endsWith("/") ? "" : "/") + path;
        }
        
        // Ensure the file name is included in the HDFS path
        String fileName = file.getOriginalFilename();
        String hdfsPath = fullPath + (fullPath.endsWith("/") ? "" : "/") + fileName;

        try (InputStream is = file.getInputStream()) {
            artifactService.uploadArtifact(hdfsPath, is);
        }

        return Map.of("path", hdfsPath);
    }

    @GetMapping("/download")
    public void downloadArtifact(@RequestParam("run_id") String runId,
                                @RequestParam("path") String path,
                                HttpServletResponse response) throws IOException {
        Run run = runService.getRun(runId);
        if (run == null) {
            throw new IllegalArgumentException("Run not found: " + runId);
        }

        String baseUri = run.getInfo().getArtifactUri();
        String hdfsPath = baseUri + (baseUri.endsWith("/") ? "" : "/") + path;

        response.setContentType("application/octet-stream");
        response.setHeader("Content-Disposition", "attachment; filename=\"" + path.substring(path.lastIndexOf("/") + 1) + "\"");

        try (OutputStream os = response.getOutputStream()) {
            artifactService.downloadArtifact(hdfsPath, os);
        }
    }

    @lombok.Data
    public static class DeleteArtifactRequest {
        private String run_id;
        private String path;
    }

    @PostMapping("/delete")
    public Map<String, Object> deleteArtifact(@RequestBody DeleteArtifactRequest request) throws IOException {
        String runId = request.getRun_id();
        String path = request.getPath();
        log.info("REST request to delete artifact for run: {}, path: {}", runId, path);
        
        Run run = runService.getRun(runId);
        if (run == null) {
            throw new IllegalArgumentException("Run not found: " + runId);
        }

        String baseUri = run.getInfo().getArtifactUri();
        String hdfsPath = baseUri + (baseUri.endsWith("/") ? "" : "/") + path;
        
        artifactService.deleteArtifact(hdfsPath);
        return Map.of();
    }
}
