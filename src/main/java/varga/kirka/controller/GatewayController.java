package varga.kirka.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import varga.kirka.model.*;
import varga.kirka.service.GatewayEndpointService;
import varga.kirka.service.GatewayRouteService;
import varga.kirka.service.GatewaySecretService;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api")
public class GatewayController {

    @Autowired
    private GatewaySecretService gatewaySecretService;

    @Autowired
    private GatewayRouteService gatewayRouteService;

    @Autowired
    private GatewayEndpointService gatewayEndpointService;

    @lombok.Data
    public static class CreateSecretRequest {
        private String secret_name;
        private List<SecretValueEntry> secret_value;
        private String provider;
        private List<AuthConfigEntry> auth_config;
        private String created_by;
    }

    @lombok.Data
    public static class UpdateSecretRequest {
        private String secret_id;
        private List<SecretValueEntry> secret_value;
        private List<AuthConfigEntry> auth_config;
        private String updated_by;
    }

    @lombok.Data
    public static class DeleteSecretRequest {
        private String secret_id;
    }

    @lombok.Data
    public static class GetSecretRequest {
        private String secret_id;
        private String secret_name;
    }

    @PostMapping("/2.0/mlflow/gateway/secrets/create")
    public Map<String, GatewaySecretInfo> createSecret(@RequestBody CreateSecretRequest request) {
        GatewaySecretInfo secret = gatewaySecretService.createSecret(
                request.secret_name,
                request.secret_value,
                request.provider,
                request.auth_config,
                request.created_by
        );
        return Map.of("secret", secret);
    }

    @PostMapping("/2.0/mlflow/gateway/secrets/update")
    public Map<String, GatewaySecretInfo> updateSecret(@RequestBody UpdateSecretRequest request) {
        GatewaySecretInfo secret = gatewaySecretService.updateSecret(
                request.secret_id,
                request.secret_value,
                request.auth_config,
                request.updated_by
        );
        return Map.of("secret", secret);
    }

    @DeleteMapping("/2.0/mlflow/gateway/secrets/delete")
    public void deleteSecret(@RequestBody DeleteSecretRequest request) {
        gatewaySecretService.deleteSecret(request.secret_id);
    }

    @GetMapping("/2.0/mlflow/gateway/secrets/get")
    public Map<String, GatewaySecretInfo> getSecret(
            @RequestParam(required = false) String secret_id,
            @RequestParam(required = false) String secret_name) {
        GatewaySecretInfo secret = gatewaySecretService.getSecret(secret_id, secret_name);
        return Map.of("secret", secret);
    }

    @GetMapping("/2.0/mlflow/gateway/secrets/list")
    public Map<String, List<GatewaySecretInfo>> listSecrets(@RequestParam(required = false) String provider) {
        List<GatewaySecretInfo> secrets = gatewaySecretService.listSecrets(provider);
        return Map.of("secrets", secrets);
    }

    @PostMapping("/2.0/mlflow/gateway/routes")
    public Map<String, GatewayRoute> createRoute(@RequestBody GatewayRoute route) {
        log.info("Creating gateway route: {}", route.getName());
        GatewayRoute created = gatewayRouteService.createRoute(route);
        return Map.of("route", created);
    }

    @GetMapping("/2.0/mlflow/gateway/routes")
    public Map<String, List<GatewayRoute>> listRoutes() {
        List<GatewayRoute> routes = gatewayRouteService.listRoutes();
        return Map.of("routes", routes);
    }

    @GetMapping("/2.0/mlflow/gateway/routes/{name}")
    public Map<String, GatewayRoute> getRoute(@PathVariable String name) {
        GatewayRoute route = gatewayRouteService.getRoute(name);
        if (route != null) {
            return Map.of("route", route);
        }
        return Map.of();
    }

    @lombok.Data
    public static class GatewayQueryRequest {
        private String name;
        private Map<String, Object> body;
    }

    @lombok.Data
    @lombok.AllArgsConstructor
    public static class GatewayQueryResponse {
        private List<Map<String, String>> candidates;
    }

    @PostMapping("/2.0/mlflow/gateway/query/{name}")
    public GatewayQueryResponse queryRoute(@PathVariable String name, @RequestBody Map<String, Object> request) {
        // log.info("Querying gateway route: {}", name);
        // Mock response for gateway query
        return new GatewayQueryResponse(List.of(
                Map.of("text", "Ceci est une réponse simulée du gateway pour " + name)
        ));
    }

    @PatchMapping("/2.0/mlflow/gateway/routes/{name}")
    public Map<String, GatewayRoute> updateRoute(@PathVariable String name, @RequestBody Map<String, Object> request) {
        log.info("Updating gateway route: {}", name);
        GatewayRoute updated = gatewayRouteService.updateRoute(name, request);
        if (updated != null) {
            return Map.of("route", updated);
        }
        return Map.of();
    }

    @lombok.Data
    public static class AttachModelRequest {
        private String endpoint_id;
        private String model_definition_id;
        private GatewayModelLinkageType linkage_type;
        private float weight;
        private Integer fallback_order;
        private String created_by;
    }

    @PostMapping("/2.0/mlflow/gateway/endpoints/models/attach")
    public Map<String, GatewayEndpointModelMapping> attachModel(@RequestBody AttachModelRequest request) {
        log.info("Attaching model to endpoint: {}", request.getEndpoint_id());
        GatewayEndpointModelConfig config = GatewayEndpointModelConfig.builder()
                .modelDefinitionId(request.getModel_definition_id())
                .linkageType(request.getLinkage_type())
                .weight(request.getWeight())
                .fallbackOrder(request.getFallback_order())
                .build();
        GatewayEndpointModelMapping mapping = gatewayEndpointService.attachModel(
                request.getEndpoint_id(), config, request.getCreated_by());
        return Map.of("mapping", mapping);
    }

    @lombok.Data
    public static class DetachModelRequest {
        private String endpoint_id;
        private String mapping_id;
    }

    @PostMapping("/2.0/mlflow/gateway/endpoints/models/detach")
    public void detachModel(@RequestBody DetachModelRequest request) {
        log.info("Detaching model {} from endpoint {}", request.getMapping_id(), request.getEndpoint_id());
        gatewayEndpointService.detachModel(request.getEndpoint_id(), request.getMapping_id());
    }

    @lombok.Data
    public static class SetTagRequest {
        private String endpoint_id;
        private String key;
        private String value;
    }

    @PostMapping("/2.0/mlflow/gateway/endpoints/set-tag")
    public void setTag(@RequestBody SetTagRequest request) {
        log.info("Setting tag {}={} on endpoint {}", request.getKey(), request.getValue(), request.getEndpoint_id());
        GatewayEndpointTag tag = GatewayEndpointTag.builder()
                .key(request.getKey())
                .value(request.getValue())
                .build();
        gatewayEndpointService.setTag(request.getEndpoint_id(), tag);
    }

    @PostMapping("/invocations")
    public Map<String, Object> invocations(@RequestBody Map<String, Object> request) {
        return Map.of("predictions", List.of());
    }

    @GetMapping("/ping")
    public Map<String, Object> ping() {
        return Map.of("status", "healthy");
    }

    @GetMapping("/version")
    public Map<String, Object> version() {
        return Map.of("version", "1.0.0");
    }

    @GetMapping("/metadata")
    public Map<String, Object> metadata() {
        return Map.of("model_name", "mock-model");
    }

    @lombok.Data
    public static class DeleteTagRequest {
        private String endpoint_id;
        private String key;
    }

    @DeleteMapping("/2.0/mlflow/gateway/endpoints/delete-tag")
    public void deleteTag(@RequestParam(required = false) String endpoint_id, 
                          @RequestParam(required = false) String key, 
                          @RequestBody(required = false) DeleteTagRequest body) {
        String eid = endpoint_id;
        String k = key;
        if (body != null) {
            if (eid == null) eid = body.getEndpoint_id();
            if (k == null) k = body.getKey();
        }
        log.info("Deleting tag {} from endpoint {}", k, eid);
        gatewayEndpointService.deleteTag(eid, k);
    }
}
