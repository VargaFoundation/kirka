package varga.kirka.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ModelVersion {
    private String name;
    private String version;
    private long creationTimestamp;
    private long lastUpdatedTimestamp;
    private String description;
    private String userId;
    private String currentStage;
    private String source;
    private String runId;
    private String status;
    private String statusMessage;
}
