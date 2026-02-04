package varga.kirka.repo;
import lombok.extern.slf4j.Slf4j;
import varga.kirka.model.Experiment;
import varga.kirka.model.ExperimentTag;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Repository
public class ExperimentRepository {

    private static final String TABLE_NAME = "mlflow_experiments";
    private static final byte[] CF_INFO = Bytes.toBytes("info");
    private static final byte[] COL_NAME = Bytes.toBytes("name");
    private static final byte[] COL_ARTIFACT_LOCATION = Bytes.toBytes("artifact_location");
    private static final byte[] COL_LIFECYCLE_STAGE = Bytes.toBytes("lifecycle_stage");
    private static final byte[] COL_CREATION_TIME = Bytes.toBytes("creation_time");
    private static final byte[] COL_LAST_UPDATE_TIME = Bytes.toBytes("last_update_time");
    private static final byte[] COL_OWNER = Bytes.toBytes("owner");
    private static final byte[] CF_TAGS = Bytes.toBytes("tags");

    @Autowired
    private Connection connection;

    public void createExperiment(Experiment experiment) throws IOException {
        log.info("HBase: creating experiment {} ({})", experiment.getName(), experiment.getExperimentId());
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
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
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Scan scan = new Scan();
            scan.setFilter(new org.apache.hadoop.hbase.filter.SingleColumnValueFilter(
                    CF_INFO, COL_NAME, org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.EQUAL, Bytes.toBytes(name)));
            try (ResultScanner scanner = table.getScanner(scan)) {
                Result result = scanner.next();
                if (result == null) return null;
                return mapResultToExperiment(result);
            }
        }
    }

    public void updateExperiment(String experimentId, String newName) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Put put = new Put(Bytes.toBytes(experimentId));
            put.addColumn(CF_INFO, COL_NAME, Bytes.toBytes(newName));
            put.addColumn(CF_INFO, COL_LAST_UPDATE_TIME, Bytes.toBytes(System.currentTimeMillis()));
            table.put(put);
        }
    }

    public void deleteExperiment(String experimentId) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Put put = new Put(Bytes.toBytes(experimentId));
            put.addColumn(CF_INFO, COL_LIFECYCLE_STAGE, Bytes.toBytes("deleted"));
            put.addColumn(CF_INFO, COL_LAST_UPDATE_TIME, Bytes.toBytes(System.currentTimeMillis()));
            table.put(put);
        }
    }

    public void restoreExperiment(String experimentId) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
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
