package varga.kirka.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import varga.kirka.model.*;
import varga.kirka.repo.GatewayEndpointRepository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
public class GatewayEndpointService {

    @Autowired
    private GatewayEndpointRepository gatewayEndpointRepository;

    public GatewayEndpointModelMapping attachModel(String endpointId, GatewayEndpointModelConfig config, String createdBy) {
        try {
            GatewayEndpoint endpoint = gatewayEndpointRepository.getEndpointById(endpointId);
            if (endpoint == null) {
                endpoint = GatewayEndpoint.builder()
                        .endpointId(endpointId)
                        .createdAt(System.currentTimeMillis())
                        .lastUpdatedAt(System.currentTimeMillis())
                        .createdBy(createdBy)
                        .lastUpdatedBy(createdBy)
                        .modelMappings(new ArrayList<>())
                        .tags(new ArrayList<>())
                        .build();
            }

            GatewayEndpointModelMapping mapping = GatewayEndpointModelMapping.builder()
                    .mappingId(UUID.randomUUID().toString())
                    .endpointId(endpointId)
                    .modelDefinitionId(config.getModelDefinitionId())
                    .weight(config.getWeight())
                    .linkageType(config.getLinkageType())
                    .fallbackOrder(config.getFallbackOrder())
                    .createdAt(System.currentTimeMillis())
                    .createdBy(createdBy)
                    .build();

            List<GatewayEndpointModelMapping> mappings = endpoint.getModelMappings();
            if (mappings == null) {
                mappings = new ArrayList<>();
            }
            mappings.add(mapping);
            endpoint.setModelMappings(mappings);
            endpoint.setLastUpdatedAt(System.currentTimeMillis());
            endpoint.setLastUpdatedBy(createdBy);

            gatewayEndpointRepository.saveEndpoint(endpoint);
            return mapping;
        } catch (IOException e) {
            log.error("Failed to attach model to endpoint in HBase", e);
            throw new RuntimeException("Failed to attach model", e);
        }
    }

    public void detachModel(String endpointId, String mappingId) {
        try {
            GatewayEndpoint endpoint = gatewayEndpointRepository.getEndpointById(endpointId);
            if (endpoint == null) {
                throw new RuntimeException("Endpoint not found: " + endpointId);
            }

            List<GatewayEndpointModelMapping> mappings = endpoint.getModelMappings();
            if (mappings != null) {
                mappings.removeIf(m -> mappingId.equals(m.getMappingId()));
                endpoint.setModelMappings(mappings);
                endpoint.setLastUpdatedAt(System.currentTimeMillis());
                gatewayEndpointRepository.saveEndpoint(endpoint);
            }
        } catch (IOException e) {
            log.error("Failed to detach model from endpoint in HBase", e);
            throw new RuntimeException("Failed to detach model", e);
        }
    }

    public void setTag(String endpointId, GatewayEndpointTag tag) {
        try {
            GatewayEndpoint endpoint = gatewayEndpointRepository.getEndpointById(endpointId);
            if (endpoint == null) {
                endpoint = GatewayEndpoint.builder()
                        .endpointId(endpointId)
                        .createdAt(System.currentTimeMillis())
                        .lastUpdatedAt(System.currentTimeMillis())
                        .modelMappings(new ArrayList<>())
                        .tags(new ArrayList<>())
                        .build();
            }

            List<GatewayEndpointTag> tags = endpoint.getTags();
            if (tags == null) {
                tags = new ArrayList<>();
            }

            tags.removeIf(t -> tag.getKey().equals(t.getKey()));
            tags.add(tag);
            endpoint.setTags(tags);
            endpoint.setLastUpdatedAt(System.currentTimeMillis());

            gatewayEndpointRepository.saveEndpoint(endpoint);
        } catch (IOException e) {
            log.error("Failed to set tag on endpoint in HBase", e);
            throw new RuntimeException("Failed to set tag", e);
        }
    }

    public void deleteTag(String endpointId, String key) {
        try {
            GatewayEndpoint endpoint = gatewayEndpointRepository.getEndpointById(endpointId);
            if (endpoint == null) {
                return;
            }

            List<GatewayEndpointTag> tags = endpoint.getTags();
            if (tags != null) {
                tags.removeIf(t -> key.equals(t.getKey()));
                endpoint.setTags(tags);
                endpoint.setLastUpdatedAt(System.currentTimeMillis());
                gatewayEndpointRepository.saveEndpoint(endpoint);
            }
        } catch (IOException e) {
            log.error("Failed to delete tag from endpoint in HBase", e);
            throw new RuntimeException("Failed to delete tag", e);
        }
    }

    public GatewayEndpoint getEndpoint(String endpointId) {
        try {
            return gatewayEndpointRepository.getEndpointById(endpointId);
        } catch (IOException e) {
            log.error("Failed to get endpoint from HBase", e);
            throw new RuntimeException("Failed to get endpoint", e);
        }
    }

    public List<GatewayEndpoint> listEndpoints() {
        try {
            return gatewayEndpointRepository.listEndpoints();
        } catch (IOException e) {
            log.error("Failed to list endpoints from HBase", e);
            throw new RuntimeException("Failed to list endpoints", e);
        }
    }
}
