package varga.kirka.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class RegisteredModel {
    private String name;
    private long creationTimestamp;
    private long lastUpdatedTimestamp;
    private String description;
    private java.util.List<ModelVersion> latestVersions;
}
