package varga.kirka.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class HBaseConfig {

    @Value("${hbase.zookeeper.quorum}")
    private String zookeeperQuorum;

    @Value("${hbase.zookeeper.property.clientPort}")
    private String zookeeperClientPort;

    @Value("${security.kerberos.enabled:false}")
    private boolean kerberosEnabled;

    @Value("${security.kerberos.principal:}")
    private String principal;

    @Value("${security.kerberos.keytab:}")
    private String keytab;

    @Value("${security.kerberos.krb5conf:}")
    private String krb5conf;

    @Bean
    public Connection hbaseConnection() throws IOException {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zookeeperQuorum);
        config.set("hbase.zookeeper.property.clientPort", zookeeperClientPort);

        if (kerberosEnabled) {
            if (krb5conf != null && !krb5conf.isEmpty()) {
                System.setProperty("java.security.krb5.conf", krb5conf);
            }
            config.set("hadoop.security.authentication", "kerberos");
            config.set("hbase.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(config);
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
        }

        return ConnectionFactory.createConnection(config);
    }
}
