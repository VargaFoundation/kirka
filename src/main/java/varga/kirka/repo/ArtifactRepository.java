package varga.kirka.repo;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

@Repository
public class ArtifactRepository {

    @Autowired
    private FileSystem fileSystem;

    public void uploadArtifact(String hdfsPath, InputStream inputStream) throws IOException {
        Path path = new Path(hdfsPath);
        try (FSDataOutputStream outputStream = fileSystem.create(path, true)) {
            IOUtils.copyBytes(inputStream, outputStream, 4096, true);
        }
    }

    public void downloadArtifact(String hdfsPath, OutputStream outputStream) throws IOException {
        Path path = new Path(hdfsPath);
        try (FSDataInputStream inputStream = fileSystem.open(path)) {
            IOUtils.copyBytes(inputStream, outputStream, 4096, false);
        }
    }

    public List<FileStatus> listArtifacts(String hdfsPath) throws IOException {
        List<FileStatus> fileStatuses = new ArrayList<>();
        Path path = new Path(hdfsPath);
        if (fileSystem.exists(path)) {
            FileStatus[] statuses = fileSystem.listStatus(path);
            if (statuses != null) {
                for (FileStatus status : statuses) {
                    fileStatuses.add(status);
                }
            }
        }
        return fileStatuses;
    }

    public boolean exists(String hdfsPath) throws IOException {
        return fileSystem.exists(new Path(hdfsPath));
    }

    public void deleteArtifact(String hdfsPath) throws IOException {
        fileSystem.delete(new Path(hdfsPath), true);
    }
}
