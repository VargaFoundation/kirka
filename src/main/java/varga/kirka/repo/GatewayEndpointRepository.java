package varga.kirka.repo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import varga.kirka.model.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Repository
public class GatewayEndpointRepository {

    private static final String TABLE_NAME = "mlflow_gateway_endpoints";
    private static final byte[] CF_INFO = Bytes.toBytes("info");

    private static final byte[] COL_ENDPOINT_ID = Bytes.toBytes("endpoint_id");
    private static final byte[] COL_NAME = Bytes.toBytes("name");
    private static final byte[] COL_CREATED_AT = Bytes.toBytes("created_at");
    private static final byte[] COL_LAST_UPDATED_AT = Bytes.toBytes("last_updated_at");
    private static final byte[] COL_CREATED_BY = Bytes.toBytes("created_by");
    private static final byte[] COL_LAST_UPDATED_BY = Bytes.toBytes("last_updated_by");
    private static final byte[] COL_MODEL_MAPPINGS = Bytes.toBytes("model_mappings");
    private static final byte[] COL_TAGS = Bytes.toBytes("tags");
    private static final byte[] COL_ROUTING_STRATEGY = Bytes.toBytes("routing_strategy");
    private static final byte[] COL_FALLBACK_CONFIG = Bytes.toBytes("fallback_config");

    @Autowired
    private Connection connection;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public void saveEndpoint(GatewayEndpoint endpoint) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Put put = new Put(Bytes.toBytes(endpoint.getEndpointId()));

            put.addColumn(CF_INFO, COL_ENDPOINT_ID, Bytes.toBytes(endpoint.getEndpointId()));

            if (endpoint.getName() != null) {
                put.addColumn(CF_INFO, COL_NAME, Bytes.toBytes(endpoint.getName()));
            }
            put.addColumn(CF_INFO, COL_CREATED_AT, Bytes.toBytes(endpoint.getCreatedAt()));
            put.addColumn(CF_INFO, COL_LAST_UPDATED_AT, Bytes.toBytes(endpoint.getLastUpdatedAt()));

            if (endpoint.getCreatedBy() != null) {
                put.addColumn(CF_INFO, COL_CREATED_BY, Bytes.toBytes(endpoint.getCreatedBy()));
            }
            if (endpoint.getLastUpdatedBy() != null) {
                put.addColumn(CF_INFO, COL_LAST_UPDATED_BY, Bytes.toBytes(endpoint.getLastUpdatedBy()));
            }
            if (endpoint.getModelMappings() != null) {
                put.addColumn(CF_INFO, COL_MODEL_MAPPINGS, Bytes.toBytes(objectMapper.writeValueAsString(endpoint.getModelMappings())));
            }
            if (endpoint.getTags() != null) {
                put.addColumn(CF_INFO, COL_TAGS, Bytes.toBytes(objectMapper.writeValueAsString(endpoint.getTags())));
            }
            if (endpoint.getRoutingStrategy() != null) {
                put.addColumn(CF_INFO, COL_ROUTING_STRATEGY, Bytes.toBytes(objectMapper.writeValueAsString(endpoint.getRoutingStrategy())));
            }
            if (endpoint.getFallbackConfig() != null) {
                put.addColumn(CF_INFO, COL_FALLBACK_CONFIG, Bytes.toBytes(objectMapper.writeValueAsString(endpoint.getFallbackConfig())));
            }

            table.put(put);
        }
    }

    public GatewayEndpoint getEndpointById(String endpointId) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Get get = new Get(Bytes.toBytes(endpointId));
            get.addFamily(CF_INFO);
            Result result = table.get(get);
            if (result.isEmpty()) return null;
            return mapResultToEndpoint(result);
        }
    }

    public List<GatewayEndpoint> listEndpoints() throws IOException {
        List<GatewayEndpoint> endpoints = new ArrayList<>();
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Scan scan = new Scan();
            scan.addFamily(CF_INFO);
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    endpoints.add(mapResultToEndpoint(result));
                }
            }
        }
        return endpoints;
    }

    public void deleteEndpoint(String endpointId) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Delete delete = new Delete(Bytes.toBytes(endpointId));
            table.delete(delete);
        }
    }

    private GatewayEndpoint mapResultToEndpoint(Result result) throws JsonProcessingException {
        GatewayEndpoint endpoint = new GatewayEndpoint();

        endpoint.setEndpointId(Bytes.toString(result.getRow()));

        byte[] nameBytes = result.getValue(CF_INFO, COL_NAME);
        if (nameBytes != null) {
            endpoint.setName(Bytes.toString(nameBytes));
        }

        byte[] createdAtBytes = result.getValue(CF_INFO, COL_CREATED_AT);
        if (createdAtBytes != null) {
            endpoint.setCreatedAt(Bytes.toLong(createdAtBytes));
        }

        byte[] lastUpdatedAtBytes = result.getValue(CF_INFO, COL_LAST_UPDATED_AT);
        if (lastUpdatedAtBytes != null) {
            endpoint.setLastUpdatedAt(Bytes.toLong(lastUpdatedAtBytes));
        }

        byte[] createdByBytes = result.getValue(CF_INFO, COL_CREATED_BY);
        if (createdByBytes != null) {
            endpoint.setCreatedBy(Bytes.toString(createdByBytes));
        }

        byte[] lastUpdatedByBytes = result.getValue(CF_INFO, COL_LAST_UPDATED_BY);
        if (lastUpdatedByBytes != null) {
            endpoint.setLastUpdatedBy(Bytes.toString(lastUpdatedByBytes));
        }

        byte[] modelMappingsBytes = result.getValue(CF_INFO, COL_MODEL_MAPPINGS);
        if (modelMappingsBytes != null) {
            endpoint.setModelMappings(objectMapper.readValue(Bytes.toString(modelMappingsBytes), new TypeReference<List<GatewayEndpointModelMapping>>() {}));
        }

        byte[] tagsBytes = result.getValue(CF_INFO, COL_TAGS);
        if (tagsBytes != null) {
            endpoint.setTags(objectMapper.readValue(Bytes.toString(tagsBytes), new TypeReference<List<GatewayEndpointTag>>() {}));
        }

        byte[] routingStrategyBytes = result.getValue(CF_INFO, COL_ROUTING_STRATEGY);
        if (routingStrategyBytes != null) {
            endpoint.setRoutingStrategy(objectMapper.readValue(Bytes.toString(routingStrategyBytes), RoutingStrategy.class));
        }

        byte[] fallbackConfigBytes = result.getValue(CF_INFO, COL_FALLBACK_CONFIG);
        if (fallbackConfigBytes != null) {
            endpoint.setFallbackConfig(objectMapper.readValue(Bytes.toString(fallbackConfigBytes), FallbackConfig.class));
        }

        return endpoint;
    }
}
