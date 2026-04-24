package varga.kirka.repo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import varga.kirka.model.*;
import varga.kirka.util.HBaseResults;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Repository
@RequiredArgsConstructor
public class ModelRegistryRepository {

    private static final String MODELS_TABLE = "mlflow_registered_models";
    private static final String VERSIONS_TABLE = "mlflow_model_versions";

    private static final byte[] CF_INFO = Bytes.toBytes("info");
    private static final byte[] CF_ALIASES = Bytes.toBytes("aliases");

    private static final byte[] COL_NAME = Bytes.toBytes("name");
    private static final byte[] COL_VERSION = Bytes.toBytes("version");
    private static final byte[] COL_CREATION_TIMESTAMP = Bytes.toBytes("creation_timestamp");
    private static final byte[] COL_LAST_UPDATED_TIMESTAMP = Bytes.toBytes("last_updated_timestamp");
    private static final byte[] COL_DESCRIPTION = Bytes.toBytes("description");
    private static final byte[] COL_USER_ID = Bytes.toBytes("user_id");
    private static final byte[] COL_CURRENT_STAGE = Bytes.toBytes("current_stage");
    private static final byte[] COL_SOURCE = Bytes.toBytes("source");
    private static final byte[] COL_RUN_ID = Bytes.toBytes("run_id");
    private static final byte[] COL_STATUS = Bytes.toBytes("status");

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
            return mapResultToRegisteredModel(result, name);
        }
    }

    private RegisteredModel mapResultToRegisteredModel(Result result, String fallbackName) throws IOException {
        String name = HBaseResults.getStringOrDefault(result, CF_INFO, COL_NAME, fallbackName);
        return RegisteredModel.builder()
                .name(name)
                .creationTimestamp(HBaseResults.getLongOrDefault(result, CF_INFO, COL_CREATION_TIMESTAMP, 0L))
                .lastUpdatedTimestamp(HBaseResults.getLongOrDefault(result, CF_INFO, COL_LAST_UPDATED_TIMESTAMP, 0L))
                .description(HBaseResults.getStringOrNull(result, CF_INFO, COL_DESCRIPTION))
                .userId(HBaseResults.getStringOrNull(result, CF_INFO, COL_USER_ID))
                .latestVersions(getLatestVersions(name))
                .aliases(extractAliases(result))
                .build();
    }

    private List<RegisteredModelAlias> extractAliases(Result result) {
        List<RegisteredModelAlias> aliases = new ArrayList<>();
        java.util.NavigableMap<byte[], byte[]> map = result.getFamilyMap(CF_ALIASES);
        if (map == null) return aliases;
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            aliases.add(RegisteredModelAlias.builder()
                    .alias(Bytes.toString(entry.getKey()))
                    .version(Bytes.toString(entry.getValue()))
                    .build());
        }
        return aliases;
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
        return listRegisteredModelsPaged(Integer.MAX_VALUE, null).items();
    }

    public Page<RegisteredModel> listRegisteredModelsPaged(int maxResults, PageToken pageToken) throws IOException {
        List<RegisteredModel> models = new ArrayList<>();
        byte[] lastRow = null;
        Scan scan = new Scan();
        scan.setCaching(Math.min(maxResults, 500));
        if (pageToken != null) {
            scan.withStartRow(pageToken.nextStartRow(), true);
        }
        try (Table table = connection.getTable(TableName.valueOf(MODELS_TABLE));
             ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {
                if (models.size() >= maxResults) break;
                String fallback = Bytes.toString(result.getRow());
                models.add(mapResultToRegisteredModel(result, fallback));
                lastRow = result.getRow();
            }
        }
        String next = (models.size() == maxResults && lastRow != null) ? PageToken.of(lastRow).encode() : null;
        return new Page<>(models, next);
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

    /** Sets (or overwrites) an alias on a registered model. */
    public void setAlias(String name, String alias, String version) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(MODELS_TABLE))) {
            Put put = new Put(Bytes.toBytes(name));
            put.addColumn(CF_ALIASES, Bytes.toBytes(alias), Bytes.toBytes(version));
            put.addColumn(CF_INFO, COL_LAST_UPDATED_TIMESTAMP, Bytes.toBytes(System.currentTimeMillis()));
            table.put(put);
        }
    }

    public void deleteAlias(String name, String alias) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(MODELS_TABLE))) {
            Delete delete = new Delete(Bytes.toBytes(name));
            delete.addColumns(CF_ALIASES, Bytes.toBytes(alias));
            table.delete(delete);
        }
    }

    /** Resolves an alias to a version string, or returns {@code null} if the alias is unknown. */
    public String getAliasedVersion(String name, String alias) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(MODELS_TABLE))) {
            Get get = new Get(Bytes.toBytes(name));
            get.addColumn(CF_ALIASES, Bytes.toBytes(alias));
            Result result = table.get(get);
            return HBaseResults.getStringOrNull(result, CF_ALIASES, Bytes.toBytes(alias));
        }
    }

    /**
     * Renames a registered model. HBase rows are immutable so this is a copy-then-delete:
     * we claim the new name atomically, duplicate the primary row and all versions, then delete
     * the source rows. On failure the newly claimed row is rolled back.
     */
    public void renameRegisteredModel(String oldName, String newName) throws IOException {
        if (oldName.equals(newName)) return;

        try (Table modelsTable = connection.getTable(TableName.valueOf(MODELS_TABLE));
             Table versionsTable = connection.getTable(TableName.valueOf(VERSIONS_TABLE))) {

            // Check destination is free
            Get destCheck = new Get(Bytes.toBytes(newName));
            destCheck.addColumn(CF_INFO, COL_NAME);
            if (!modelsTable.get(destCheck).isEmpty()) {
                throw new ExperimentAlreadyExistsException(newName);
            }

            Result source = modelsTable.get(new Get(Bytes.toBytes(oldName)));
            if (source.isEmpty()) {
                return;
            }

            Put copy = new Put(Bytes.toBytes(newName));
            copy.addColumn(CF_INFO, COL_NAME, Bytes.toBytes(newName));
            long creation = HBaseResults.getLongOrDefault(source, CF_INFO, COL_CREATION_TIMESTAMP, System.currentTimeMillis());
            copy.addColumn(CF_INFO, COL_CREATION_TIMESTAMP, Bytes.toBytes(creation));
            copy.addColumn(CF_INFO, COL_LAST_UPDATED_TIMESTAMP, Bytes.toBytes(System.currentTimeMillis()));
            String description = HBaseResults.getStringOrNull(source, CF_INFO, COL_DESCRIPTION);
            if (description != null) copy.addColumn(CF_INFO, COL_DESCRIPTION, Bytes.toBytes(description));
            String userId = HBaseResults.getStringOrNull(source, CF_INFO, COL_USER_ID);
            if (userId != null) copy.addColumn(CF_INFO, COL_USER_ID, Bytes.toBytes(userId));
            for (var entry : source.getFamilyMap(CF_ALIASES) != null
                    ? source.getFamilyMap(CF_ALIASES).entrySet()
                    : java.util.Collections.<Map.Entry<byte[], byte[]>>emptySet()) {
                copy.addColumn(CF_ALIASES, entry.getKey(), entry.getValue());
            }

            modelsTable.put(copy);

            try {
                // Copy versions (each row key is "oldName_versionNumber")
                Scan versionScan = new Scan();
                versionScan.setRowPrefixFilter(Bytes.toBytes(oldName + "_"));
                versionScan.setCaching(100);
                List<Put> versionCopies = new ArrayList<>();
                List<Delete> versionDeletes = new ArrayList<>();
                try (ResultScanner scanner = versionsTable.getScanner(versionScan)) {
                    for (Result vr : scanner) {
                        String oldKey = Bytes.toString(vr.getRow());
                        String versionNumber = oldKey.substring(oldName.length() + 1);
                        Put p = new Put(Bytes.toBytes(newName + "_" + versionNumber));
                        for (var fam : vr.getNoVersionMap().entrySet()) {
                            byte[] family = fam.getKey();
                            for (var q : fam.getValue().entrySet()) {
                                byte[] qualifier = q.getKey();
                                if (java.util.Arrays.equals(family, CF_INFO) && java.util.Arrays.equals(qualifier, COL_NAME)) {
                                    p.addColumn(family, qualifier, Bytes.toBytes(newName));
                                } else {
                                    p.addColumn(family, qualifier, q.getValue());
                                }
                            }
                        }
                        versionCopies.add(p);
                        versionDeletes.add(new Delete(vr.getRow()));
                    }
                }
                if (!versionCopies.isEmpty()) versionsTable.put(versionCopies);
                if (!versionDeletes.isEmpty()) versionsTable.delete(versionDeletes);

                // Finally drop the old registered-model row
                modelsTable.delete(new Delete(Bytes.toBytes(oldName)));
            } catch (IOException | RuntimeException e) {
                try {
                    modelsTable.delete(new Delete(Bytes.toBytes(newName)));
                } catch (IOException rollback) {
                    log.error("Failed to roll back renamed model {} -> {}", oldName, newName, rollback);
                }
                throw e;
            }
        }
    }

    /**
     * Scans the model-versions table. When {@code modelName} is provided, only versions of that
     * model are returned (via prefix scan). The optional {@code stageFilter} (e.g. "Production")
     * narrows the result further server-side.
     */
    public List<ModelVersion> searchModelVersions(String modelName, String stageFilter, int maxResults) throws IOException {
        List<ModelVersion> versions = new ArrayList<>();
        try (Table table = connection.getTable(TableName.valueOf(VERSIONS_TABLE))) {
            Scan scan = new Scan();
            scan.setCaching(100);
            if (modelName != null && !modelName.isBlank()) {
                scan.setRowPrefixFilter(Bytes.toBytes(modelName + "_"));
            }
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    ModelVersion mv = mapResultToModelVersion(result);
                    if (stageFilter == null || stageFilter.isBlank()
                            || stageFilter.equalsIgnoreCase(mv.getCurrentStage())) {
                        versions.add(mv);
                        if (versions.size() >= maxResults) break;
                    }
                }
            }
        }
        return versions;
    }
}
