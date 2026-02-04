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
public class GatewayRoute {
    private String name;
    private String routeType;
    private String modelName;
    private String modelProvider;
    private Map<String, Object> config;
    /** Owner of the route (user who created it) */
    private String createdBy;
}
