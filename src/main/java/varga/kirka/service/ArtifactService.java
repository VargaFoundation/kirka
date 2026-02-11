package varga.kirka.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import varga.kirka.model.FileInfo;
import varga.kirka.repo.ArtifactRepository;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ArtifactService {

    private final ArtifactRepository artifactRepository;

    public List<FileInfo> listArtifacts(String path) throws IOException {
        // log.debug("Listing artifacts at: {}", path);
        return artifactRepository.listArtifacts(path);
    }

    public void uploadArtifact(String path, java.io.InputStream inputStream) throws IOException {
        // log.info("Uploading artifact to: {}", path);
        artifactRepository.uploadArtifact(path, inputStream);
    }

    public void downloadArtifact(String path, java.io.OutputStream outputStream) throws IOException {
        // log.info("Downloading artifact from: {}", path);
        artifactRepository.downloadArtifact(path, outputStream);
    }

    public void deleteArtifact(String path) throws IOException {
        // log.info("Deleting artifact at: {}", path);
        artifactRepository.deleteArtifact(path);
    }
}
