package varga.kirka.repo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import java.io.IOException;

@SpringBootTest(properties = {
    "spring.main.allow-bean-definition-overriding=true",
    "security.kerberos.enabled=false"
})
public abstract class AbstractHBaseIntegrationTest {

    protected static HBaseTestingUtility utility;

    @BeforeAll
    public static void setupCluster() throws Exception {
        // Hadoop/HBase may call JAAS `Subject.getSubject(...)` which is gated behind the (deprecated) SecurityManager
        // (see JEP 411). Sur JDK récents, il faut lancer la JVM de test avec : -Djava.security.manager=allow
        Assumptions.assumeTrue(
                "allow".equals(System.getProperty("java.security.manager")),
                "Tests HBase ignorés : ajouter -Djava.security.manager=allow (configuré via Maven Surefire dans le pom)."
        );

        utility = new HBaseTestingUtility();
        Configuration conf = utility.getConfiguration();
        // Désactiver les interfaces Web pour éviter les conflits Jetty
        conf.setInt("hbase.master.info.port", -1);
        conf.setInt("hbase.regionserver.info.port", -1);
        conf.setBoolean("dfs.namenode.http-enabled", false);
        conf.setBoolean("dfs.datanode.http.enabled", false);
        
        // Utiliser le système de fichiers local si possible pour simplifier
        // utility.startMiniCluster(1);
        
        // Alternative plus légère si startMiniCluster échoue à cause de Jetty dans HDFS
        utility.startMiniZKCluster();
        int zkPort = utility.getConfiguration().getInt("hbase.zookeeper.property.clientPort", 2181);
        System.setProperty("hbase.zookeeper.quorum", "127.0.0.1");
        System.setProperty("hbase.zookeeper.property.clientPort", String.valueOf(zkPort));
        utility.startMiniHBaseCluster(1, 1);
        
        createTables();
    }

    private static void createTables() throws IOException {
        String[] tables = {"mlflow_experiments", "mlflow_runs", "mlflow_metric_history", "mlflow_registered_models", "mlflow_model_versions", "mlflow_scorers", "mlflow_prompts"};
        for (String table : tables) {
            utility.createTable(TableName.valueOf(table), new byte[][]{Bytes.toBytes("info"), Bytes.toBytes("params"), Bytes.toBytes("metrics"), Bytes.toBytes("tags")});
            utility.waitTableEnabled(TableName.valueOf(table), 60000);
        }
    }

    @AfterAll
    public static void tearDownCluster() throws Exception {
        if (utility != null) {
            utility.shutdownMiniCluster();
        }
    }

    @TestConfiguration
    static class HBaseTestConfig {
        @Bean("fileSystem")
        @Primary
        public org.apache.hadoop.fs.FileSystem testFileSystem() throws IOException {
            return utility.getTestFileSystem();
        }
    }
}
