package varga.kirka.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Experiment {
    private String experimentId;
    private String name;
    private String artifactLocation;
    private String lifecycleStage;
    private long creationTime;
    private long lastUpdateTime;
}
