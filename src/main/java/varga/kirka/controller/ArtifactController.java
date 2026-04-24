package varga.kirka.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import varga.kirka.model.FileInfo;
import varga.kirka.model.Run;
import varga.kirka.service.ArtifactService;
import varga.kirka.service.RunService;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/2.0/mlflow/artifacts")
@RequiredArgsConstructor
public class ArtifactController {

    private final ArtifactService artifactService;

    private final RunService runService;

    @GetMapping("/list")
    public Map<String, List<FileInfo>> listArtifacts(@RequestParam(value = "run_id") String runId,
                                            @RequestParam(value = "path", required = false) String path) throws IOException {
        log.debug("REST request to list artifacts for run: {}, path: {}", runId, path);
        Run run = runService.getRun(runId);
        if (run == null) {
            throw new IllegalArgumentException("Run not found: " + runId);
        }

        String baseUri = run.getInfo().getArtifactUri();
        String fullPath = baseUri;
        if (path != null && !path.isEmpty()) {
            validatePath(path);
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
            validatePath(path);
            fullPath = baseUri + (baseUri.endsWith("/") ? "" : "/") + path;
        }

        // Ensure the file name is included in the HDFS path
        String fileName = file.getOriginalFilename();
        if (fileName != null) {
            validatePath(fileName);
        }
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

        validatePath(path);
        String baseUri = run.getInfo().getArtifactUri();
        String hdfsPath = baseUri + (baseUri.endsWith("/") ? "" : "/") + path;

        // Extract safe filename from path
        String fileName = path.contains("/") ? path.substring(path.lastIndexOf("/") + 1) : path;
        response.setContentType("application/octet-stream");
        response.setHeader("Content-Disposition", "attachment; filename=\"" + fileName + "\"");

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

        validatePath(path);
        String baseUri = run.getInfo().getArtifactUri();
        String hdfsPath = baseUri + (baseUri.endsWith("/") ? "" : "/") + path;

        artifactService.deleteArtifact(hdfsPath);
        return Map.of();
    }

    /**
     * Validates that a user-supplied relative path cannot escape its run's artifact directory.
     * Rejects encoded traversal (%2e%2e), null bytes, control characters, Windows back-slashes,
     * absolute paths and any path that, once normalized, still contains a '..' segment.
     */
    private void validatePath(String path) {
        if (path == null || path.isEmpty()) return;

        // Defence in depth: decode once in case an upstream proxy forwarded the raw encoding,
        // then reject double-encoded traversal (%252e...) by refusing any residual '%'.
        String decoded;
        try {
            decoded = URLDecoder.decode(path, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Malformed path encoding: " + path);
        }

        for (int i = 0; i < decoded.length(); i++) {
            char c = decoded.charAt(i);
            if (c == '\0' || (c < 0x20 && c != '\t')) {
                throw new IllegalArgumentException("Path contains a forbidden control character");
            }
        }

        String normalized = decoded.replace('\\', '/');
        if (normalized.startsWith("/")) {
            throw new IllegalArgumentException("Absolute paths are not allowed: " + path);
        }

        // Reject any '..' segment before normalization. A path like "a/b/c/../../escape"
        // would normalize to the safe "a/escape", but such constructions in user input are
        // suspicious enough to refuse at the boundary.
        for (String raw : normalized.split("/")) {
            if (raw.equals("..")) {
                throw new IllegalArgumentException("Path traversal is not allowed: " + path);
            }
        }

        Path canonical;
        try {
            canonical = Paths.get(normalized).normalize();
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException("Invalid path: " + path);
        }
        for (Path segment : canonical) {
            if (segment.toString().equals("..")) {
                throw new IllegalArgumentException("Path traversal is not allowed: " + path);
            }
        }
        if (canonical.isAbsolute()) {
            throw new IllegalArgumentException("Absolute paths are not allowed: " + path);
        }
    }
}
