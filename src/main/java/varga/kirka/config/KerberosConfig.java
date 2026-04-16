package varga.kirka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Centralized Kerberos authentication for all Hadoop services (HDFS, HBase).
 * Performs a single keytab login with automatic ticket renewal enabled,
 * so that long-running HDFS/HBase connections never expire.
 */
@Slf4j
@org.springframework.context.annotation.Configuration
@ConditionalOnProperty(name = "security.kerberos.enabled", havingValue = "true")
public class KerberosConfig {

    @Value("${security.kerberos.principal}")
    private String principal;

    @Value("${security.kerberos.keytab}")
    private String keytab;

    @Value("${security.kerberos.krb5conf:}")
    private String krb5conf;

    @Bean
    public UserGroupInformation kerberosUgi() throws Exception {
        if (krb5conf != null && !krb5conf.isEmpty()) {
            System.setProperty("java.security.krb5.conf", krb5conf);
        }

        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        conf.setBoolean("hadoop.kerberos.keytab.login.autorenewal.enabled", true);
        UserGroupInformation.setConfiguration(conf);

        log.info("Kerberos login with principal={}, keytab={}, auto-renewal=true", principal, keytab);
        UserGroupInformation.loginUserFromKeytab(principal, keytab);

        UserGroupInformation ugi = UserGroupInformation.getLoginUser();
        log.info("Kerberos login successful: {}", ugi.getUserName());
        return ugi;
    }
}
