package varga.kirka.repo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import varga.kirka.model.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Repository
@RequiredArgsConstructor
public class ModelRegistryRepository {

    private static final String MODELS_TABLE = "mlflow_registered_models";
    private static final String VERSIONS_TABLE = "mlflow_model_versions";
    
    private static final byte[] CF_INFO = Bytes.toBytes("info");
    
    private final Connection connection;

    public void createRegisteredModel(String name) throws IOException {
        log.info("HBase: creating registered model {}", name);
        try (Table table = connection.getTable(TableName.valueOf(MODELS_TABLE))) {
            Put put = new Put(Bytes.toBytes(name));
            put.addColumn(CF_INFO, Bytes.toBytes("name"), Bytes.toBytes(name));
            put.addColumn(CF_INFO, Bytes.toBytes("creation_timestamp"), Bytes.toBytes(System.currentTimeMillis()));
            put.addColumn(CF_INFO, Bytes.toBytes("last_updated_timestamp"), Bytes.toBytes(System.currentTimeMillis()));
            table.put(put);
        }
    }

    public RegisteredModel getRegisteredModel(String name) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(MODELS_TABLE))) {
            Get get = new Get(Bytes.toBytes(name));
            Result result = table.get(get);
            if (result.isEmpty()) return null;
            
            byte[] nameBytes = result.getValue(CF_INFO, Bytes.toBytes("name"));
            byte[] creationTimestamp = result.getValue(CF_INFO, Bytes.toBytes("creation_timestamp"));
            byte[] lastUpdatedTimestamp = result.getValue(CF_INFO, Bytes.toBytes("last_updated_timestamp"));

            return RegisteredModel.builder()
                    .name(nameBytes != null ? Bytes.toString(nameBytes) : name)
                    .creationTimestamp(creationTimestamp != null ? Bytes.toLong(creationTimestamp) : 0L)
                    .lastUpdatedTimestamp(lastUpdatedTimestamp != null ? Bytes.toLong(lastUpdatedTimestamp) : 0L)
                    .latestVersions(getLatestVersions(name))
                    .build();
        }
    }

    public void createModelVersion(ModelVersion version) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(VERSIONS_TABLE))) {
            // Row key: name + version
            byte[] rowKey = Bytes.toBytes(version.getName() + "_" + version.getVersion());
            Put put = new Put(rowKey);
            put.addColumn(CF_INFO, Bytes.toBytes("name"), Bytes.toBytes(version.getName()));
            put.addColumn(CF_INFO, Bytes.toBytes("version"), Bytes.toBytes(version.getVersion()));
            put.addColumn(CF_INFO, Bytes.toBytes("creation_timestamp"), Bytes.toBytes(version.getCreationTimestamp()));
            put.addColumn(CF_INFO, Bytes.toBytes("current_stage"), Bytes.toBytes(version.getCurrentStage()));
            put.addColumn(CF_INFO, Bytes.toBytes("source"), Bytes.toBytes(version.getSource()));
            put.addColumn(CF_INFO, Bytes.toBytes("run_id"), Bytes.toBytes(version.getRunId()));
            put.addColumn(CF_INFO, Bytes.toBytes("status"), Bytes.toBytes("READY"));
            table.put(put);
        }
        
        // Update last_updated_timestamp of registered model
        try (Table table = connection.getTable(TableName.valueOf(MODELS_TABLE))) {
            Put put = new Put(Bytes.toBytes(version.getName()));
            put.addColumn(CF_INFO, Bytes.toBytes("last_updated_timestamp"), Bytes.toBytes(System.currentTimeMillis()));
            table.put(put);
        }
    }

    private List<ModelVersion> getLatestVersions(String name) throws IOException {
        List<ModelVersion> versions = new ArrayList<>();
        try (Table table = connection.getTable(TableName.valueOf(VERSIONS_TABLE))) {
            Scan scan = new Scan();
            scan.setRowPrefixFilter(Bytes.toBytes(name + "_"));
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    versions.add(mapResultToModelVersion(result));
                }
            }
        }
        return versions;
    }

    private ModelVersion mapResultToModelVersion(Result result) {
        byte[] name = result.getValue(CF_INFO, Bytes.toBytes("name"));
        byte[] version = result.getValue(CF_INFO, Bytes.toBytes("version"));
        byte[] creationTimestamp = result.getValue(CF_INFO, Bytes.toBytes("creation_timestamp"));
        byte[] lastUpdatedTimestamp = result.getValue(CF_INFO, Bytes.toBytes("last_updated_timestamp"));
        byte[] description = result.getValue(CF_INFO, Bytes.toBytes("description"));
        byte[] userId = result.getValue(CF_INFO, Bytes.toBytes("user_id"));
        byte[] currentStage = result.getValue(CF_INFO, Bytes.toBytes("current_stage"));
        byte[] source = result.getValue(CF_INFO, Bytes.toBytes("source"));
        byte[] runId = result.getValue(CF_INFO, Bytes.toBytes("run_id"));
        byte[] status = result.getValue(CF_INFO, Bytes.toBytes("status"));

        String statusStr = status != null ? Bytes.toString(status) : null;
        ModelVersionStatus modelStatus = ModelVersionStatus.READY;
        if (statusStr != null) {
            try {
                modelStatus = ModelVersionStatus.valueOf(statusStr);
            } catch (IllegalArgumentException e) {
                log.warn("Invalid model status: {}", statusStr);
            }
        }

        return ModelVersion.builder()
                .name(name != null ? Bytes.toString(name) : null)
                .version(version != null ? Bytes.toString(version) : null)
                .creationTimestamp(creationTimestamp != null ? Bytes.toLong(creationTimestamp) : 0L)
                .lastUpdatedTimestamp(lastUpdatedTimestamp != null ? Bytes.toLong(lastUpdatedTimestamp) : 0L)
                .description(description != null ? Bytes.toString(description) : null)
                .userId(userId != null ? Bytes.toString(userId) : null)
                .currentStage(currentStage != null ? Bytes.toString(currentStage) : null)
                .source(source != null ? Bytes.toString(source) : null)
                .runId(runId != null ? Bytes.toString(runId) : null)
                .status(modelStatus)
                .build();
    }
    
    public List<RegisteredModel> listRegisteredModels() throws IOException {
        List<RegisteredModel> models = new ArrayList<>();
        try (Table table = connection.getTable(TableName.valueOf(MODELS_TABLE))) {
            try (ResultScanner scanner = table.getScanner(new Scan())) {
                for (Result result : scanner) {
                    byte[] nameBytes = result.getValue(CF_INFO, Bytes.toBytes("name"));
                    byte[] creationTimestamp = result.getValue(CF_INFO, Bytes.toBytes("creation_timestamp"));
                    byte[] lastUpdatedTimestamp = result.getValue(CF_INFO, Bytes.toBytes("last_updated_timestamp"));
                    byte[] description = result.getValue(CF_INFO, Bytes.toBytes("description"));
                    
                    String name = nameBytes != null ? Bytes.toString(nameBytes) : Bytes.toString(result.getRow());
                    
                    models.add(RegisteredModel.builder()
                            .name(name)
                            .creationTimestamp(creationTimestamp != null ? Bytes.toLong(creationTimestamp) : 0L)
                            .lastUpdatedTimestamp(lastUpdatedTimestamp != null ? Bytes.toLong(lastUpdatedTimestamp) : 0L)
                            .description(description != null ? Bytes.toString(description) : null)
                            .latestVersions(getLatestVersions(name))
                            .build());
                }
            }
        }
        return models;
    }
    public void updateRegisteredModel(String name, String description) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(MODELS_TABLE))) {
            Put put = new Put(Bytes.toBytes(name));
            if (description != null) {
                put.addColumn(CF_INFO, Bytes.toBytes("description"), Bytes.toBytes(description));
            }
            put.addColumn(CF_INFO, Bytes.toBytes("last_updated_timestamp"), Bytes.toBytes(System.currentTimeMillis()));
            table.put(put);
        }
    }

    public void deleteRegisteredModel(String name) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(MODELS_TABLE))) {
            Delete delete = new Delete(Bytes.toBytes(name));
            table.delete(delete);
        }
        // Should also delete versions, but MLFlow might keep them or mark them.
        // For simplicity, we just delete the model.
    }

    public ModelVersion getModelVersion(String name, String version) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(VERSIONS_TABLE))) {
            Get get = new Get(Bytes.toBytes(name + "_" + version));
            Result result = table.get(get);
            if (result.isEmpty()) return null;
            return mapResultToModelVersion(result);
        }
    }

    public void updateModelVersion(String name, String version, String description) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(VERSIONS_TABLE))) {
            Put put = new Put(Bytes.toBytes(name + "_" + version));
            if (description != null) {
                put.addColumn(CF_INFO, Bytes.toBytes("description"), Bytes.toBytes(description));
            }
            table.put(put);
        }
    }

    public void deleteModelVersion(String name, String version) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(VERSIONS_TABLE))) {
            Delete delete = new Delete(Bytes.toBytes(name + "_" + version));
            table.delete(delete);
        }
    }

    public List<ModelVersion> getVersions(String name) throws IOException {
        return getLatestVersions(name);
    }

    public void updateModelVersionStage(String name, String version, String stage) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(VERSIONS_TABLE))) {
            Put put = new Put(Bytes.toBytes(name + "_" + version));
            put.addColumn(CF_INFO, Bytes.toBytes("current_stage"), Bytes.toBytes(stage));
            table.put(put);
        }
    }

    public void setRegisteredModelTag(String name, String key, String value) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(MODELS_TABLE))) {
            Put put = new Put(Bytes.toBytes(name));
            put.addColumn(Bytes.toBytes("tags"), Bytes.toBytes(key), Bytes.toBytes(value));
            table.put(put);
        }
    }

    public void deleteRegisteredModelTag(String name, String key) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(MODELS_TABLE))) {
            Delete delete = new Delete(Bytes.toBytes(name));
            delete.addColumns(Bytes.toBytes("tags"), Bytes.toBytes(key));
            table.delete(delete);
        }
    }

    public void setModelVersionTag(String name, String version, String key, String value) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(VERSIONS_TABLE))) {
            Put put = new Put(Bytes.toBytes(name + "_" + version));
            put.addColumn(Bytes.toBytes("tags"), Bytes.toBytes(key), Bytes.toBytes(value));
            table.put(put);
        }
    }

    public void deleteModelVersionTag(String name, String version, String key) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(VERSIONS_TABLE))) {
            Delete delete = new Delete(Bytes.toBytes(name + "_" + version));
            delete.addColumns(Bytes.toBytes("tags"), Bytes.toBytes(key));
            table.delete(delete);
        }
    }
}
