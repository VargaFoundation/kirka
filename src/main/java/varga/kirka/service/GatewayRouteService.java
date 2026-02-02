package varga.kirka.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import varga.kirka.model.GatewayRoute;
import varga.kirka.repo.GatewayRouteRepository;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class GatewayRouteService {

    @Autowired
    private GatewayRouteRepository gatewayRouteRepository;

    public GatewayRoute createRoute(GatewayRoute route) {
        try {
            gatewayRouteRepository.saveRoute(route);
        } catch (IOException e) {
            log.error("Failed to save route to HBase", e);
            throw new RuntimeException("Failed to create route", e);
        }
        return route;
    }

    public GatewayRoute getRoute(String name) {
        try {
            return gatewayRouteRepository.getRouteByName(name);
        } catch (IOException e) {
            log.error("Failed to get route from HBase", e);
            throw new RuntimeException("Failed to get route", e);
        }
    }

    public List<GatewayRoute> listRoutes() {
        try {
            return gatewayRouteRepository.listRoutes();
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
            gatewayRouteRepository.deleteRoute(name);
        } catch (IOException e) {
            log.error("Failed to delete route from HBase", e);
            throw new RuntimeException("Failed to delete route", e);
        }
    }
}
