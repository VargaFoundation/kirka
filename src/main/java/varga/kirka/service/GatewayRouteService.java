package varga.kirka.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import varga.kirka.model.GatewayRoute;
import varga.kirka.repo.GatewayRouteRepository;
import varga.kirka.security.SecurityContextHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class GatewayRouteService {

    private static final String RESOURCE_TYPE = "route";

    @Autowired
    private GatewayRouteRepository gatewayRouteRepository;

    @Autowired
    private SecurityContextHelper securityContextHelper;

    public GatewayRoute createRoute(GatewayRoute route) {
        try {
            // Set current user as owner
            String currentUser = securityContextHelper.getCurrentUser();
            route.setCreatedBy(currentUser);
            gatewayRouteRepository.saveRoute(route);
        } catch (IOException e) {
            log.error("Failed to save route to HBase", e);
            throw new RuntimeException("Failed to create route", e);
        }
        return route;
    }

    public GatewayRoute getRoute(String name) {
        try {
            GatewayRoute route = gatewayRouteRepository.getRouteByName(name);
            if (route != null) {
                securityContextHelper.checkReadAccess(RESOURCE_TYPE, name, route.getCreatedBy(), Map.of());
            }
            return route;
        } catch (IOException e) {
            log.error("Failed to get route from HBase", e);
            throw new RuntimeException("Failed to get route", e);
        }
    }

    public List<GatewayRoute> listRoutes() {
        try {
            List<GatewayRoute> routes = gatewayRouteRepository.listRoutes();
            // Filter routes based on read access
            return routes.stream()
                    .filter(route -> securityContextHelper.canRead(RESOURCE_TYPE, route.getName(), 
                            route.getCreatedBy(), Map.of()))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            log.error("Failed to list routes from HBase", e);
            throw new RuntimeException("Failed to list routes", e);
        }
    }

    public GatewayRoute updateRoute(String name, Map<String, Object> updates) {
        try {
            GatewayRoute existing = gatewayRouteRepository.getRouteByName(name);
            if (existing == null) {
                throw new RuntimeException("Route not found: " + name);
            }

            // Check write access
            securityContextHelper.checkWriteAccess(RESOURCE_TYPE, name, existing.getCreatedBy(), Map.of());

            if (updates.containsKey("route_type")) {
                existing.setRouteType((String) updates.get("route_type"));
            }
            if (updates.containsKey("model_name")) {
                existing.setModelName((String) updates.get("model_name"));
            }
            if (updates.containsKey("model_provider")) {
                existing.setModelProvider((String) updates.get("model_provider"));
            }
            if (updates.containsKey("config")) {
                @SuppressWarnings("unchecked")
                Map<String, Object> config = (Map<String, Object>) updates.get("config");
                existing.setConfig(config);
            }

            gatewayRouteRepository.saveRoute(existing);
            return existing;
        } catch (IOException e) {
            log.error("Failed to update route in HBase", e);
            throw new RuntimeException("Failed to update route", e);
        }
    }

    public void deleteRoute(String name) {
        try {
            GatewayRoute route = gatewayRouteRepository.getRouteByName(name);
            if (route != null) {
                securityContextHelper.checkDeleteAccess(RESOURCE_TYPE, name, route.getCreatedBy(), Map.of());
            }
            gatewayRouteRepository.deleteRoute(name);
        } catch (IOException e) {
            log.error("Failed to delete route from HBase", e);
            throw new RuntimeException("Failed to delete route", e);
        }
    }
}
