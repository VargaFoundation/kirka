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
import varga.kirka.model.AuthConfigEntry;
import varga.kirka.model.GatewaySecretInfo;
import varga.kirka.model.MaskedValuesEntry;
import varga.kirka.model.SecretValueEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Repository
public class GatewaySecretRepository {

    private static final String TABLE_NAME = "mlflow_gateway_secrets";
    private static final byte[] CF_INFO = Bytes.toBytes("info");
    private static final byte[] CF_VALUES = Bytes.toBytes("values");

    private static final byte[] COL_SECRET_NAME = Bytes.toBytes("secret_name");
    private static final byte[] COL_MASKED_VALUES = Bytes.toBytes("masked_values");
    private static final byte[] COL_CREATED_AT = Bytes.toBytes("created_at");
    private static final byte[] COL_LAST_UPDATED_AT = Bytes.toBytes("last_updated_at");
    private static final byte[] COL_PROVIDER = Bytes.toBytes("provider");
    private static final byte[] COL_CREATED_BY = Bytes.toBytes("created_by");
    private static final byte[] COL_LAST_UPDATED_BY = Bytes.toBytes("last_updated_by");
    private static final byte[] COL_AUTH_CONFIG = Bytes.toBytes("auth_config");
    private static final byte[] COL_SECRET_VALUES = Bytes.toBytes("secret_values");

    @Autowired
    private Connection connection;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public void saveSecret(GatewaySecretInfo secretInfo, List<SecretValueEntry> secretValues) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Put put = new Put(Bytes.toBytes(secretInfo.getSecretId()));

            put.addColumn(CF_INFO, COL_SECRET_NAME, Bytes.toBytes(secretInfo.getSecretName()));
            put.addColumn(CF_INFO, COL_CREATED_AT, Bytes.toBytes(secretInfo.getCreatedAt()));
            put.addColumn(CF_INFO, COL_LAST_UPDATED_AT, Bytes.toBytes(secretInfo.getLastUpdatedAt()));

            if (secretInfo.getProvider() != null) {
                put.addColumn(CF_INFO, COL_PROVIDER, Bytes.toBytes(secretInfo.getProvider()));
            }
            if (secretInfo.getCreatedBy() != null) {
                put.addColumn(CF_INFO, COL_CREATED_BY, Bytes.toBytes(secretInfo.getCreatedBy()));
            }
            if (secretInfo.getLastUpdatedBy() != null) {
                put.addColumn(CF_INFO, COL_LAST_UPDATED_BY, Bytes.toBytes(secretInfo.getLastUpdatedBy()));
            }
            if (secretInfo.getMaskedValues() != null) {
                put.addColumn(CF_INFO, COL_MASKED_VALUES, Bytes.toBytes(objectMapper.writeValueAsString(secretInfo.getMaskedValues())));
            }
            if (secretInfo.getAuthConfig() != null) {
                put.addColumn(CF_INFO, COL_AUTH_CONFIG, Bytes.toBytes(objectMapper.writeValueAsString(secretInfo.getAuthConfig())));
            }

            if (secretValues != null) {
                put.addColumn(CF_VALUES, COL_SECRET_VALUES, Bytes.toBytes(objectMapper.writeValueAsString(secretValues)));
            }

            table.put(put);
        }
    }

    public GatewaySecretInfo getSecretById(String secretId) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Get get = new Get(Bytes.toBytes(secretId));
            get.addFamily(CF_INFO);
            Result result = table.get(get);
            if (result.isEmpty()) return null;
            return mapResultToSecretInfo(result);
        }
    }

    public GatewaySecretInfo getSecretByName(String secretName) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Scan scan = new Scan();
            scan.addFamily(CF_INFO);
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    String name = Bytes.toString(result.getValue(CF_INFO, COL_SECRET_NAME));
                    if (secretName.equals(name)) {
                        return mapResultToSecretInfo(result);
                    }
                }
            }
        }
        return null;
    }

    public List<SecretValueEntry> getSecretValues(String secretId) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Get get = new Get(Bytes.toBytes(secretId));
            get.addFamily(CF_VALUES);
            Result result = table.get(get);
            if (result.isEmpty()) return null;

            byte[] valuesBytes = result.getValue(CF_VALUES, COL_SECRET_VALUES);
            if (valuesBytes != null) {
                return objectMapper.readValue(Bytes.toString(valuesBytes), new TypeReference<List<SecretValueEntry>>() {});
            }
        }
        return null;
    }

    public List<GatewaySecretInfo> listSecrets(String provider) throws IOException {
        List<GatewaySecretInfo> secrets = new ArrayList<>();
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Scan scan = new Scan();
            scan.addFamily(CF_INFO);
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    GatewaySecretInfo secretInfo = mapResultToSecretInfo(result);
                    if (provider == null || provider.equals(secretInfo.getProvider())) {
                        secrets.add(secretInfo);
                    }
                }
            }
        }
        return secrets;
    }

    public void deleteSecret(String secretId) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Delete delete = new Delete(Bytes.toBytes(secretId));
            table.delete(delete);
        }
    }

    private GatewaySecretInfo mapResultToSecretInfo(Result result) throws JsonProcessingException {
        GatewaySecretInfo secretInfo = new GatewaySecretInfo();
        secretInfo.setSecretId(Bytes.toString(result.getRow()));
        secretInfo.setSecretName(Bytes.toString(result.getValue(CF_INFO, COL_SECRET_NAME)));

        byte[] createdAtBytes = result.getValue(CF_INFO, COL_CREATED_AT);
        if (createdAtBytes != null) {
            secretInfo.setCreatedAt(Bytes.toLong(createdAtBytes));
        }

        byte[] lastUpdatedAtBytes = result.getValue(CF_INFO, COL_LAST_UPDATED_AT);
        if (lastUpdatedAtBytes != null) {
            secretInfo.setLastUpdatedAt(Bytes.toLong(lastUpdatedAtBytes));
        }

        byte[] providerBytes = result.getValue(CF_INFO, COL_PROVIDER);
        if (providerBytes != null) {
            secretInfo.setProvider(Bytes.toString(providerBytes));
        }

        byte[] createdByBytes = result.getValue(CF_INFO, COL_CREATED_BY);
        if (createdByBytes != null) {
            secretInfo.setCreatedBy(Bytes.toString(createdByBytes));
        }

        byte[] lastUpdatedByBytes = result.getValue(CF_INFO, COL_LAST_UPDATED_BY);
        if (lastUpdatedByBytes != null) {
            secretInfo.setLastUpdatedBy(Bytes.toString(lastUpdatedByBytes));
        }

        byte[] maskedValuesBytes = result.getValue(CF_INFO, COL_MASKED_VALUES);
        if (maskedValuesBytes != null) {
            secretInfo.setMaskedValues(objectMapper.readValue(Bytes.toString(maskedValuesBytes), new TypeReference<List<MaskedValuesEntry>>() {}));
        }

        byte[] authConfigBytes = result.getValue(CF_INFO, COL_AUTH_CONFIG);
        if (authConfigBytes != null) {
            secretInfo.setAuthConfig(objectMapper.readValue(Bytes.toString(authConfigBytes), new TypeReference<List<AuthConfigEntry>>() {}));
        }

        return secretInfo;
    }
}
