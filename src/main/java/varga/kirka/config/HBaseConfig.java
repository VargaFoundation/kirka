package varga.kirka.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

@Configuration
public class HBaseConfig {

    @Value("${hbase.zookeeper.quorum}")
    private String zookeeperQuorum;

    @Value("${hbase.zookeeper.property.clientPort}")
    private String zookeeperClientPort;

    @Autowired(required = false)
    private UserGroupInformation kerberosUgi;

    @Bean
    public Connection hbaseConnection() throws IOException, InterruptedException {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zookeeperQuorum);
        config.set("hbase.zookeeper.property.clientPort", zookeeperClientPort);

        if (kerberosUgi != null) {
            config.set("hadoop.security.authentication", "kerberos");
            config.set("hbase.security.authentication", "kerberos");
            return kerberosUgi.doAs((PrivilegedExceptionAction<Connection>) () ->
                    ConnectionFactory.createConnection(config));
        }

        return ConnectionFactory.createConnection(config);
    }
}
