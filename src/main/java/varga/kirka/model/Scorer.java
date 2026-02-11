package varga.kirka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Scorer {
    private String experimentId;
    private String scorerName;
    private int scorerVersion;
    private String serializedScorer;
    private long creationTime;
    private String scorerId;
    /** Owner of the scorer (user who created it) */
    private String owner;
}
