package varga.kirka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import varga.kirka.repo.ArtifactRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Slf4j
@Service
public class ArtifactService {

    @Autowired
    private ArtifactRepository artifactRepository;

    public List<FileStatus> listArtifacts(String path) throws IOException {
        log.debug("Listing artifacts at: {}", path);
        return artifactRepository.listArtifacts(path);
    }

    public void uploadArtifact(String path, java.io.InputStream inputStream) throws IOException {
        log.info("Uploading artifact to: {}", path);
        artifactRepository.uploadArtifact(path, inputStream);
    }

    public void downloadArtifact(String path, java.io.OutputStream outputStream) throws IOException {
        log.info("Downloading artifact from: {}", path);
        artifactRepository.downloadArtifact(path, outputStream);
    }

    public void deleteArtifact(String path) throws IOException {
        log.info("Deleting artifact at: {}", path);
        artifactRepository.deleteArtifact(path);
    }
}
