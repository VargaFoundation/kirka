package varga.kirka.repo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.springframework.stereotype.Repository;

import varga.kirka.model.FileInfo;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Repository
@RequiredArgsConstructor
public class ArtifactRepository {

    private final FileSystem fileSystem;

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

    public List<FileInfo> listArtifacts(String hdfsPath) throws IOException {
        List<FileInfo> fileInfos = new ArrayList<>();
        Path path = new Path(hdfsPath);
        if (fileSystem.exists(path)) {
            FileStatus[] statuses = fileSystem.listStatus(path);
            if (statuses != null) {
                for (FileStatus status : statuses) {
                    fileInfos.add(FileInfo.builder()
                            .path(status.getPath().toString())
                            .isDir(status.isDirectory())
                            .fileSize(status.isDirectory() ? null : status.getLen())
                            .build());
                }
            }
        }
        return fileInfos;
    }

    public boolean exists(String hdfsPath) throws IOException {
        return fileSystem.exists(new Path(hdfsPath));
    }

    public void deleteArtifact(String hdfsPath) throws IOException {
        fileSystem.delete(new Path(hdfsPath), true);
    }
}
