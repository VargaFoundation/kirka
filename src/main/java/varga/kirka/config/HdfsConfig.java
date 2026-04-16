package varga.kirka.config;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;

@Configuration
public class HdfsConfig {

    @Value("${hadoop.hdfs.uri}")
    private String hdfsUri;

    @Autowired(required = false)
    private UserGroupInformation kerberosUgi;

    @Bean
    public FileSystem fileSystem() throws IOException, InterruptedException {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

        if (kerberosUgi != null) {
            // Create the FileSystem within the authenticated UGI context so that
            // it inherits the auto-renewed Kerberos credentials.
            conf.set("hadoop.security.authentication", "kerberos");
            return kerberosUgi.doAs((PrivilegedExceptionAction<FileSystem>) () ->
                    FileSystem.get(URI.create(hdfsUri), conf));
        }

        return FileSystem.get(URI.create(hdfsUri), conf);
    }
}
