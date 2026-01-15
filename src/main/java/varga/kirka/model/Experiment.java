package varga.kirka.model;

import lombok.Builder;
import lombok.Data;
import java.util.Map;

@Data
@Builder
public class Experiment {
    private String experimentId;
    private String name;
    private String artifactLocation;
    private String lifecycleStage;
    private long creationTime;
    private long lastUpdateTime;
    private Map<String, String> tags;
}
