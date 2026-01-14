package varga.kirka.model;

import lombok.Builder;
import lombok.Data;
import java.util.Map;

@Data
@Builder
public class Run {
    private String runId;
    private String experimentId;
    private String status;
    private long startTime;
    private long endTime;
    private String artifactUri;
    private Map<String, String> parameters;
    private Map<String, Double> metrics;
    private Map<String, String> tags;
}
