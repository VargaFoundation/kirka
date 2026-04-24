package varga.kirka.repo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import varga.kirka.model.Experiment;
import varga.kirka.model.ExperimentTag;
import varga.kirka.util.HBaseResults;
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
public class ExperimentRepository {

    private static final String TABLE_NAME = "mlflow_experiments";
    private static final String NAME_INDEX_TABLE_NAME = "mlflow_experiments_name_index";
    private static final byte[] CF_INFO = Bytes.toBytes("info");
    private static final byte[] COL_NAME = Bytes.toBytes("name");
    private static final byte[] COL_ARTIFACT_LOCATION = Bytes.toBytes("artifact_location");
    private static final byte[] COL_LIFECYCLE_STAGE = Bytes.toBytes("lifecycle_stage");
    private static final byte[] COL_CREATION_TIME = Bytes.toBytes("creation_time");
    private static final byte[] COL_LAST_UPDATE_TIME = Bytes.toBytes("last_update_time");
    private static final byte[] COL_OWNER = Bytes.toBytes("owner");
    private static final byte[] CF_TAGS = Bytes.toBytes("tags");
    private static final byte[] COL_EXPERIMENT_ID = Bytes.toBytes("experiment_id");

    private final Connection connection;

    public void createExperiment(Experiment experiment) throws IOException {
        log.info("HBase: creating experiment {} ({})", experiment.getName(), experiment.getExperimentId());

        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
             Table indexTable = connection.getTable(TableName.valueOf(NAME_INDEX_TABLE_NAME))) {

            // Claim the name slot atomically: CheckAndMutate ifNotExists. Only one concurrent
            // creator wins; the others observe the row and raise ExperimentAlreadyExistsException.
            byte[] nameRow = Bytes.toBytes(experiment.getName());
            Put indexPut = new Put(nameRow);
            indexPut.addColumn(CF_INFO, COL_EXPERIMENT_ID, Bytes.toBytes(experiment.getExperimentId()));
            CheckAndMutate claim = CheckAndMutate.newBuilder(nameRow)
                    .ifNotExists(CF_INFO, COL_EXPERIMENT_ID)
                    .build(indexPut);
            if (!indexTable.checkAndMutate(claim).isSuccess()) {
                throw new ExperimentAlreadyExistsException(experiment.getName());
            }

            try {
                Put put = new Put(Bytes.toBytes(experiment.getExperimentId()));
                put.addColumn(CF_INFO, COL_NAME, Bytes.toBytes(experiment.getName()));
                if (experiment.getArtifactLocation() != null) {
                    put.addColumn(CF_INFO, COL_ARTIFACT_LOCATION, Bytes.toBytes(experiment.getArtifactLocation()));
                }
                put.addColumn(CF_INFO, COL_LIFECYCLE_STAGE, Bytes.toBytes(experiment.getLifecycleStage()));
                put.addColumn(CF_INFO, COL_CREATION_TIME, Bytes.toBytes(experiment.getCreationTime()));
                put.addColumn(CF_INFO, COL_LAST_UPDATE_TIME, Bytes.toBytes(experiment.getLastUpdateTime()));
                if (experiment.getOwner() != null) {
                    put.addColumn(CF_INFO, COL_OWNER, Bytes.toBytes(experiment.getOwner()));
                }
                if (experiment.getTags() != null) {
                    for (ExperimentTag tag : experiment.getTags()) {
                        put.addColumn(CF_TAGS, Bytes.toBytes(tag.getKey()), Bytes.toBytes(tag.getValue()));
                    }
                }
                table.put(put);
            } catch (IOException | RuntimeException e) {
                // Main row write failed: roll back the name claim so the slot stays reusable.
                try {
                    indexTable.delete(new Delete(nameRow));
                } catch (IOException rollback) {
                    log.error("Failed to roll back name index after primary write error for experiment {}",
                            experiment.getName(), rollback);
                }
                throw e;
            }
        }
    }
    
    /**
     * Get experiment ID by name using the reversed index table.
     * @param name the experiment name
     * @return the experiment ID or null if not found
     */
    public String getExperimentIdByName(String name) throws IOException {
        try (Table indexTable = connection.getTable(TableName.valueOf(NAME_INDEX_TABLE_NAME))) {
            Get get = new Get(Bytes.toBytes(name));
            Result result = indexTable.get(get);
            if (result.isEmpty()) return null;
            byte[] idBytes = result.getValue(CF_INFO, COL_EXPERIMENT_ID);
            return idBytes != null ? Bytes.toString(idBytes) : null;
        }
    }

    public Experiment getExperiment(String experimentId) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Get get = new Get(Bytes.toBytes(experimentId));
            Result result = table.get(get);
            if (result.isEmpty()) return null;
            return mapResultToExperiment(result);
        }
    }

    public List<Experiment> listExperiments() throws IOException {
        List<Experiment> experiments = new ArrayList<>();
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
             ResultScanner scanner = table.getScanner(new Scan())) {
            for (Result result : scanner) {
                experiments.add(mapResultToExperiment(result));
            }
        }
        return experiments;
    }

    public Experiment getExperimentByName(String name) throws IOException {
        // Use the reversed index to find the experiment ID by name
        String experimentId = getExperimentIdByName(name);
        if (experimentId == null) {
            return null;
        }
        return getExperiment(experimentId);
    }

    public void updateExperiment(String experimentId, String newName) throws IOException {
        Experiment currentExperiment = getExperiment(experimentId);
        if (currentExperiment == null) {
            return;
        }
        String oldName = currentExperiment.getName();

        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
             Table indexTable = connection.getTable(TableName.valueOf(NAME_INDEX_TABLE_NAME))) {

            if (!oldName.equals(newName)) {
                // Atomically claim the new name slot before changing anything else.
                byte[] newNameRow = Bytes.toBytes(newName);
                Put claimPut = new Put(newNameRow);
                claimPut.addColumn(CF_INFO, COL_EXPERIMENT_ID, Bytes.toBytes(experimentId));
                CheckAndMutate claim = CheckAndMutate.newBuilder(newNameRow)
                        .ifNotExists(CF_INFO, COL_EXPERIMENT_ID)
                        .build(claimPut);
                if (!indexTable.checkAndMutate(claim).isSuccess()) {
                    throw new ExperimentAlreadyExistsException(newName);
                }

                try {
                    Put put = new Put(Bytes.toBytes(experimentId));
                    put.addColumn(CF_INFO, COL_NAME, Bytes.toBytes(newName));
                    put.addColumn(CF_INFO, COL_LAST_UPDATE_TIME, Bytes.toBytes(System.currentTimeMillis()));
                    table.put(put);

                    // Drop the old name only after the primary row was updated; if this last step
                    // fails the reconciler is expected to GC the stale entry on the next pass.
                    indexTable.delete(new Delete(Bytes.toBytes(oldName)));
                } catch (IOException | RuntimeException e) {
                    try {
                        indexTable.delete(new Delete(newNameRow));
                    } catch (IOException rollback) {
                        log.error("Failed to roll back new-name index after update error for experiment {}",
                                experimentId, rollback);
                    }
                    throw e;
                }
            } else {
                Put put = new Put(Bytes.toBytes(experimentId));
                put.addColumn(CF_INFO, COL_LAST_UPDATE_TIME, Bytes.toBytes(System.currentTimeMillis()));
                table.put(put);
            }
        }
    }

    public void deleteExperiment(String experimentId) throws IOException {
        // Get current experiment to find its name for index cleanup
        Experiment experiment = getExperiment(experimentId);
        if (experiment == null) {
            return;
        }

        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
             Table indexTable = connection.getTable(TableName.valueOf(NAME_INDEX_TABLE_NAME))) {
            Put put = new Put(Bytes.toBytes(experimentId));
            put.addColumn(CF_INFO, COL_LIFECYCLE_STAGE, Bytes.toBytes("deleted"));
            put.addColumn(CF_INFO, COL_LAST_UPDATE_TIME, Bytes.toBytes(System.currentTimeMillis()));
            table.put(put);

            // Remove from name index so the name can be reused
            Delete indexDelete = new Delete(Bytes.toBytes(experiment.getName()));
            indexTable.delete(indexDelete);
        }
    }

    public void restoreExperiment(String experimentId) throws IOException {
        Experiment experiment = getExperiment(experimentId);
        if (experiment == null) {
            return;
        }

        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
             Table indexTable = connection.getTable(TableName.valueOf(NAME_INDEX_TABLE_NAME))) {
            // Reclaim the name slot atomically. It's fine if we already own it (we overwrite).
            byte[] nameRow = Bytes.toBytes(experiment.getName());
            Put indexPut = new Put(nameRow);
            indexPut.addColumn(CF_INFO, COL_EXPERIMENT_ID, Bytes.toBytes(experimentId));
            CheckAndMutate claim = CheckAndMutate.newBuilder(nameRow)
                    .ifNotExists(CF_INFO, COL_EXPERIMENT_ID)
                    .build(indexPut);
            if (!indexTable.checkAndMutate(claim).isSuccess()) {
                String existingId = getExperimentIdByName(experiment.getName());
                if (existingId != null && !existingId.equals(experimentId)) {
                    throw new ExperimentAlreadyExistsException(experiment.getName());
                }
            }

            Put put = new Put(Bytes.toBytes(experimentId));
            put.addColumn(CF_INFO, COL_LIFECYCLE_STAGE, Bytes.toBytes("active"));
            put.addColumn(CF_INFO, COL_LAST_UPDATE_TIME, Bytes.toBytes(System.currentTimeMillis()));
            table.put(put);
        }
    }

    public void setExperimentTag(String experimentId, String key, String value) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Put put = new Put(Bytes.toBytes(experimentId));
            put.addColumn(CF_TAGS, Bytes.toBytes(key), Bytes.toBytes(value));
            table.put(put);
        }
    }

    private List<ExperimentTag> extractTags(Result result) {
        List<ExperimentTag> tags = new ArrayList<>();
        java.util.NavigableMap<byte[], byte[]> map = result.getFamilyMap(CF_TAGS);
        if (map != null) {
            for (java.util.Map.Entry<byte[], byte[]> entry : map.entrySet()) {
                tags.add(new ExperimentTag(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue())));
            }
        }
        return tags;
    }

    private Experiment mapResultToExperiment(Result result) {
        return Experiment.builder()
                .experimentId(Bytes.toString(result.getRow()))
                .name(HBaseResults.getStringOrNull(result, CF_INFO, COL_NAME))
                .artifactLocation(HBaseResults.getStringOrNull(result, CF_INFO, COL_ARTIFACT_LOCATION))
                .lifecycleStage(HBaseResults.getStringOrNull(result, CF_INFO, COL_LIFECYCLE_STAGE))
                .creationTime(HBaseResults.getLongOrDefault(result, CF_INFO, COL_CREATION_TIME, 0L))
                .lastUpdateTime(HBaseResults.getLongOrDefault(result, CF_INFO, COL_LAST_UPDATE_TIME, 0L))
                .tags(extractTags(result))
                .owner(HBaseResults.getStringOrNull(result, CF_INFO, COL_OWNER))
                .build();
    }
}
