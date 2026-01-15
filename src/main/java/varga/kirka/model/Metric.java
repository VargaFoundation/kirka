package varga.kirka.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Metric {
    private String key;
    private double value;
    private long timestamp;
    private long step;
}
