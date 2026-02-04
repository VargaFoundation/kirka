package varga.kirka.config;

import org.apache.hadoop.hbase.client.Connection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Health Indicator for HBase connectivity.
 * Reports the health status of the HBase connection used by Kirka.
 * 
 * This indicator checks if the HBase connection is available and not closed.
 * It will report:
 * - UP: when the connection is available and open
 * - DOWN: when the connection is null, closed, or an error occurs
 */
@Component("hbaseHealthIndicator")
public class HBaseHealthIndicator implements HealthIndicator {

    private final Connection hbaseConnection;

    /**
     * Constructor with optional HBase connection injection.
     * The connection may be null if HBase is not configured.
     *
     * @param hbaseConnection the HBase connection (may be null)
     */
    @Autowired(required = false)
    public HBaseHealthIndicator(Connection hbaseConnection) {
        this.hbaseConnection = hbaseConnection;
    }

    /**
     * Checks the health of the HBase connection.
     *
     * @return Health status with details about the HBase connection state
     */
    @Override
    public Health health() {
        if (hbaseConnection == null) {
            return Health.down()
                    .withDetail("error", "HBase connection is not configured")
                    .build();
        }

        try {
            if (hbaseConnection.isClosed()) {
                return Health.down()
                        .withDetail("error", "HBase connection is closed")
                        .build();
            }
            return Health.up()
                    .withDetail("status", "HBase connection is open and available")
                    .build();
        } catch (Exception e) {
            return Health.down()
                    .withDetail("error", "Failed to check HBase connection status")
                    .withDetail("exception", e.getMessage())
                    .build();
        }
    }
}
