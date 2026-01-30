package varga.kirka.model;

import lombok.Builder;
import lombok.Data;
import java.util.Map;

@Data
@Builder
public class Prompt {
    private String id;
    private String name;
    private String version;
    private String template;
    private String description;
    private long creationTimestamp;
    private long lastUpdatedTimestamp;
    private Map<String, String> tags;
}
