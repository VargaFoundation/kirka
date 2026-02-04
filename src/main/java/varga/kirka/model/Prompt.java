package varga.kirka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Prompt {
    private String id;
    private String name;
    private String version;
    private String template;
    private String description;
    private long creationTimestamp;
    private long lastUpdatedTimestamp;
    private Map<String, String> tags;
    /** Owner of the prompt (user who created it) */
    private String owner;
}
