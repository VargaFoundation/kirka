package varga.kirka.service;

import org.apache.hadoop.fs.FileStatus;
import varga.kirka.repo.ArtifactRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
public class ArtifactService {

    @Autowired
    private ArtifactRepository artifactRepository;

    public List<FileStatus> listArtifacts(String path) throws IOException {
        return artifactRepository.listArtifacts(path);
    }

    public void uploadArtifact(String path, java.io.InputStream inputStream) throws IOException {
        artifactRepository.uploadArtifact(path, inputStream);
    }

    public void downloadArtifact(String path, java.io.OutputStream outputStream) throws IOException {
        artifactRepository.downloadArtifact(path, outputStream);
    }
}
