package varga.kirka.model;

import lombok.Builder;
import lombok.Data;
import java.util.Map;

@Data
@Builder
public class GatewayRoute {
    private String name;
    private String routeType;
    private String modelName;
    private String modelProvider;
    private Map<String, Object> config;
}
