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
import varga.kirka.model.GatewayRoute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Repository
public class GatewayRouteRepository {

    private static final String TABLE_NAME = "mlflow_gateway_routes";
    private static final byte[] CF_INFO = Bytes.toBytes("info");

    private static final byte[] COL_NAME = Bytes.toBytes("name");
    private static final byte[] COL_ROUTE_TYPE = Bytes.toBytes("route_type");
    private static final byte[] COL_MODEL_NAME = Bytes.toBytes("model_name");
    private static final byte[] COL_MODEL_PROVIDER = Bytes.toBytes("model_provider");
    private static final byte[] COL_CONFIG = Bytes.toBytes("config");

    @Autowired
    private Connection connection;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public void saveRoute(GatewayRoute route) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Put put = new Put(Bytes.toBytes(route.getName()));

            put.addColumn(CF_INFO, COL_NAME, Bytes.toBytes(route.getName()));

            if (route.getRouteType() != null) {
                put.addColumn(CF_INFO, COL_ROUTE_TYPE, Bytes.toBytes(route.getRouteType()));
            }
            if (route.getModelName() != null) {
                put.addColumn(CF_INFO, COL_MODEL_NAME, Bytes.toBytes(route.getModelName()));
            }
            if (route.getModelProvider() != null) {
                put.addColumn(CF_INFO, COL_MODEL_PROVIDER, Bytes.toBytes(route.getModelProvider()));
            }
            if (route.getConfig() != null) {
                put.addColumn(CF_INFO, COL_CONFIG, Bytes.toBytes(objectMapper.writeValueAsString(route.getConfig())));
            }

            table.put(put);
        }
    }

    public GatewayRoute getRouteByName(String name) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Get get = new Get(Bytes.toBytes(name));
            get.addFamily(CF_INFO);
            Result result = table.get(get);
            if (result.isEmpty()) return null;
            return mapResultToRoute(result);
        }
    }

    public List<GatewayRoute> listRoutes() throws IOException {
        List<GatewayRoute> routes = new ArrayList<>();
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Scan scan = new Scan();
            scan.addFamily(CF_INFO);
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    routes.add(mapResultToRoute(result));
                }
            }
        }
        return routes;
    }

    public void deleteRoute(String name) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Delete delete = new Delete(Bytes.toBytes(name));
            table.delete(delete);
        }
    }

    private GatewayRoute mapResultToRoute(Result result) throws JsonProcessingException {
        String name = Bytes.toString(result.getValue(CF_INFO, COL_NAME));
        String routeType = null;
        String modelName = null;
        String modelProvider = null;
        Map<String, Object> config = null;

        byte[] routeTypeBytes = result.getValue(CF_INFO, COL_ROUTE_TYPE);
        if (routeTypeBytes != null) {
            routeType = Bytes.toString(routeTypeBytes);
        }

        byte[] modelNameBytes = result.getValue(CF_INFO, COL_MODEL_NAME);
        if (modelNameBytes != null) {
            modelName = Bytes.toString(modelNameBytes);
        }

        byte[] modelProviderBytes = result.getValue(CF_INFO, COL_MODEL_PROVIDER);
        if (modelProviderBytes != null) {
            modelProvider = Bytes.toString(modelProviderBytes);
        }

        byte[] configBytes = result.getValue(CF_INFO, COL_CONFIG);
        if (configBytes != null) {
            config = objectMapper.readValue(Bytes.toString(configBytes), new TypeReference<Map<String, Object>>() {});
        }

        return GatewayRoute.builder()
                .name(name)
                .routeType(routeType)
                .modelName(modelName)
                .modelProvider(modelProvider)
                .config(config)
                .build();
    }
}
