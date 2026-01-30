package varga.kirka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Dataset {
    private String name;
    private String digest;
    private String sourceType;
    private String source;
    private String schema;
    private String profile;
}
