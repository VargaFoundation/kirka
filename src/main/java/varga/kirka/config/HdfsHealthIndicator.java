package varga.kirka.config;

import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Health Indicator for HDFS connectivity.
 * Reports the health status of the HDFS file system used by Kirka.
 * 
 * This indicator checks if the HDFS file system is available and accessible.
 * It will report:
 * - UP: when the file system is available and the home directory is accessible
 * - DOWN: when the file system is null or an error occurs during access
 */
@Component("hdfsHealthIndicator")
public class HdfsHealthIndicator implements HealthIndicator {

    private final FileSystem hdfsFileSystem;

    /**
     * Constructor with optional HDFS file system injection.
     * The file system may be null if HDFS is not configured.
     *
     * @param hdfsFileSystem the HDFS file system (may be null)
     */
    @Autowired(required = false)
    public HdfsHealthIndicator(FileSystem hdfsFileSystem) {
        this.hdfsFileSystem = hdfsFileSystem;
    }

    /**
     * Checks the health of the HDFS file system.
     *
     * @return Health status with details about the HDFS connection state
     */
    @Override
    public Health health() {
        if (hdfsFileSystem == null) {
            return Health.down()
                    .withDetail("error", "HDFS file system is not configured")
                    .build();
        }

        try {
            // Verify HDFS is accessible by checking if home directory exists
            boolean accessible = hdfsFileSystem.exists(hdfsFileSystem.getHomeDirectory());
            if (accessible) {
                return Health.up()
                        .withDetail("status", "HDFS file system is accessible")
                        .withDetail("homeDirectory", hdfsFileSystem.getHomeDirectory().toString())
                        .build();
            } else {
                return Health.down()
                        .withDetail("error", "HDFS home directory is not accessible")
                        .build();
            }
        } catch (Exception e) {
            return Health.down()
                    .withDetail("error", "Failed to access HDFS file system")
                    .withDetail("exception", e.getMessage())
                    .build();
        }
    }
}
