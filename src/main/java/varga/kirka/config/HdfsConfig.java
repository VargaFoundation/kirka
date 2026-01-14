package varga.kirka.config;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.URI;

@Configuration
public class HdfsConfig {

    @Value("${hadoop.hdfs.uri}")
    private String hdfsUri;

    @Value("${security.kerberos.enabled:false}")
    private boolean kerberosEnabled;

    @Value("${security.kerberos.principal:}")
    private String principal;

    @Value("${security.kerberos.keytab:}")
    private String keytab;

    @Value("${security.kerberos.krb5conf:}")
    private String krb5conf;

    @Bean
    public FileSystem fileSystem() throws IOException {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        
        if (kerberosEnabled) {
            if (krb5conf != null && !krb5conf.isEmpty()) {
                System.setProperty("java.security.krb5.conf", krb5conf);
            }
            conf.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
        }

        return FileSystem.get(URI.create(hdfsUri), conf);
    }
}
