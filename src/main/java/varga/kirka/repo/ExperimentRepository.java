package varga.kirka.repo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import varga.kirka.model.Experiment;
import varga.kirka.model.ExperimentTag;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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
        
        // Check if experiment name already exists using the name index
        String existingId = getExperimentIdByName(experiment.getName());
        if (existingId != null) {
            throw new ExperimentAlreadyExistsException(experiment.getName());
        }
        
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
             Table indexTable = connection.getTable(TableName.valueOf(NAME_INDEX_TABLE_NAME))) {
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
            
            // Add entry to name index (name -> experiment_id)
            Put indexPut = new Put(Bytes.toBytes(experiment.getName()));
            indexPut.addColumn(CF_INFO, COL_EXPERIMENT_ID, Bytes.toBytes(experiment.getExperimentId()));
            indexTable.put(indexPut);
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
        // Get the current experiment to find the old name
        Experiment currentExperiment = getExperiment(experimentId);
        if (currentExperiment == null) {
            return;
        }
        
        String oldName = currentExperiment.getName();
        
        // If the name is changing, check uniqueness and update the index
        if (!oldName.equals(newName)) {
            // Check if new name already exists
            String existingId = getExperimentIdByName(newName);
            if (existingId != null) {
                throw new ExperimentAlreadyExistsException(newName);
            }
        }
        
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
             Table indexTable = connection.getTable(TableName.valueOf(NAME_INDEX_TABLE_NAME))) {
            Put put = new Put(Bytes.toBytes(experimentId));
            put.addColumn(CF_INFO, COL_NAME, Bytes.toBytes(newName));
            put.addColumn(CF_INFO, COL_LAST_UPDATE_TIME, Bytes.toBytes(System.currentTimeMillis()));
            table.put(put);
            
            // Update the name index if the name changed
            if (!oldName.equals(newName)) {
                // Delete old name from index
                Delete deleteOldIndex = new Delete(Bytes.toBytes(oldName));
                indexTable.delete(deleteOldIndex);
                
                // Add new name to index
                Put indexPut = new Put(Bytes.toBytes(newName));
                indexPut.addColumn(CF_INFO, COL_EXPERIMENT_ID, Bytes.toBytes(experimentId));
                indexTable.put(indexPut);
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
        // Get current experiment to restore its name in the index
        Experiment experiment = getExperiment(experimentId);
        if (experiment == null) {
            return;
        }

        // Check if the name is now taken by another experiment
        String existingId = getExperimentIdByName(experiment.getName());
        if (existingId != null && !existingId.equals(experimentId)) {
            throw new ExperimentAlreadyExistsException(experiment.getName());
        }

        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
             Table indexTable = connection.getTable(TableName.valueOf(NAME_INDEX_TABLE_NAME))) {
            Put put = new Put(Bytes.toBytes(experimentId));
            put.addColumn(CF_INFO, COL_LIFECYCLE_STAGE, Bytes.toBytes("active"));
            put.addColumn(CF_INFO, COL_LAST_UPDATE_TIME, Bytes.toBytes(System.currentTimeMillis()));
            table.put(put);

            // Re-add to name index
            Put indexPut = new Put(Bytes.toBytes(experiment.getName()));
            indexPut.addColumn(CF_INFO, COL_EXPERIMENT_ID, Bytes.toBytes(experimentId));
            indexTable.put(indexPut);
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
        byte[] ownerBytes = result.getValue(CF_INFO, COL_OWNER);
        return Experiment.builder()
                .experimentId(Bytes.toString(result.getRow()))
                .name(Bytes.toString(result.getValue(CF_INFO, COL_NAME)))
                .artifactLocation(Bytes.toString(result.getValue(CF_INFO, COL_ARTIFACT_LOCATION)))
                .lifecycleStage(Bytes.toString(result.getValue(CF_INFO, COL_LIFECYCLE_STAGE)))
                .creationTime(Bytes.toLong(result.getValue(CF_INFO, COL_CREATION_TIME)))
                .lastUpdateTime(Bytes.toLong(result.getValue(CF_INFO, COL_LAST_UPDATE_TIME)))
                .tags(extractTags(result))
                .owner(ownerBytes != null ? Bytes.toString(ownerBytes) : null)
                .build();
    }
}
