package varga.kirka.repo;

import varga.kirka.model.RegisteredModel;
import varga.kirka.model.ModelVersion;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Repository
public class ModelRegistryRepository {

    private static final String MODELS_TABLE = "mlflow_registered_models";
    private static final String VERSIONS_TABLE = "mlflow_model_versions";
    
    private static final byte[] CF_INFO = Bytes.toBytes("info");
    
    @Autowired
    private Connection connection;

    public void createRegisteredModel(String name) throws IOException {
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
            
            return RegisteredModel.builder()
                    .name(Bytes.toString(result.getValue(CF_INFO, Bytes.toBytes("name"))))
                    .creationTimestamp(Bytes.toLong(result.getValue(CF_INFO, Bytes.toBytes("creation_timestamp"))))
                    .lastUpdatedTimestamp(Bytes.toLong(result.getValue(CF_INFO, Bytes.toBytes("last_updated_timestamp"))))
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
        return ModelVersion.builder()
                .name(Bytes.toString(result.getValue(CF_INFO, Bytes.toBytes("name"))))
                .version(Bytes.toString(result.getValue(CF_INFO, Bytes.toBytes("version"))))
                .creationTimestamp(Bytes.toLong(result.getValue(CF_INFO, Bytes.toBytes("creation_timestamp"))))
                .currentStage(Bytes.toString(result.getValue(CF_INFO, Bytes.toBytes("current_stage"))))
                .source(Bytes.toString(result.getValue(CF_INFO, Bytes.toBytes("source"))))
                .runId(Bytes.toString(result.getValue(CF_INFO, Bytes.toBytes("run_id"))))
                .status(Bytes.toString(result.getValue(CF_INFO, Bytes.toBytes("status"))))
                .build();
    }
    
    public List<RegisteredModel> listRegisteredModels() throws IOException {
        List<RegisteredModel> models = new ArrayList<>();
        try (Table table = connection.getTable(TableName.valueOf(MODELS_TABLE))) {
            try (ResultScanner scanner = table.getScanner(new Scan())) {
                for (Result result : scanner) {
                    String name = Bytes.toString(result.getValue(CF_INFO, Bytes.toBytes("name")));
                    models.add(RegisteredModel.builder()
                            .name(name)
                            .creationTimestamp(Bytes.toLong(result.getValue(CF_INFO, Bytes.toBytes("creation_timestamp"))))
                            .lastUpdatedTimestamp(Bytes.toLong(result.getValue(CF_INFO, Bytes.toBytes("last_updated_timestamp"))))
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
}
