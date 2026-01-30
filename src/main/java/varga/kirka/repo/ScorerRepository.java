package varga.kirka.repo;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import varga.kirka.model.Scorer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@Repository
public class ScorerRepository {

    private static final String TABLE_NAME = "mlflow_scorers";
    private static final byte[] CF_INFO = Bytes.toBytes("info");
    private static final byte[] COL_EXPERIMENT_ID = Bytes.toBytes("experiment_id");
    private static final byte[] COL_NAME = Bytes.toBytes("name");
    private static final byte[] COL_VERSION = Bytes.toBytes("version");
    private static final byte[] COL_SERIALIZED_SCORER = Bytes.toBytes("serialized_scorer");
    private static final byte[] COL_CREATION_TIME = Bytes.toBytes("creation_time");

    @Autowired
    private Connection connection;

    public Scorer registerScorer(String experimentId, String name, String serializedScorer) throws IOException {
        int version = getNextVersion(experimentId, name);
        String scorerId = UUID.randomUUID().toString();
        long creationTime = System.currentTimeMillis();

        Scorer scorer = Scorer.builder()
                .scorerId(scorerId)
                .experimentId(Integer.parseInt(experimentId))
                .scorerName(name)
                .scorerVersion(version)
                .serializedScorer(serializedScorer)
                .creationTime(creationTime)
                .build();

        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            // Row key: experimentId_name_version
            Put put = new Put(Bytes.toBytes(experimentId + "_" + name + "_" + version));
            put.addColumn(CF_INFO, Bytes.toBytes("scorer_id"), Bytes.toBytes(scorerId));
            put.addColumn(CF_INFO, COL_EXPERIMENT_ID, Bytes.toBytes(experimentId));
            put.addColumn(CF_INFO, COL_NAME, Bytes.toBytes(name));
            put.addColumn(CF_INFO, COL_VERSION, Bytes.toBytes(version));
            put.addColumn(CF_INFO, COL_SERIALIZED_SCORER, Bytes.toBytes(serializedScorer));
            put.addColumn(CF_INFO, COL_CREATION_TIME, Bytes.toBytes(creationTime));
            table.put(put);
        }
        return scorer;
    }

    private int getNextVersion(String experimentId, String name) throws IOException {
        List<Scorer> versions = listScorerVersions(experimentId, name);
        return versions.stream()
                .mapToInt(Scorer::getScorerVersion)
                .max()
                .orElse(0) + 1;
    }

    public List<Scorer> listScorerVersions(String experimentId, String name) throws IOException {
        List<Scorer> scorers = new ArrayList<>();
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Scan scan = new Scan();
            // Start row for the prefix
            scan.withStartRow(Bytes.toBytes(experimentId + "_" + name + "_"));
            scan.withStopRow(Bytes.toBytes(experimentId + "_" + name + "_\uffff"));
            
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    scorers.add(mapResultToScorer(result));
                }
            }
        }
        return scorers;
    }

    public List<Scorer> listScorers(String experimentId) throws IOException {
        // MLFlow says: "List of scorer entities (latest version for each scorer name)."
        List<Scorer> allScorers = new ArrayList<>();
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(experimentId + "_"));
            scan.withStopRow(Bytes.toBytes(experimentId + "_\uffff"));
            
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    allScorers.add(mapResultToScorer(result));
                }
            }
        }
        
        // Group by name and keep latest version
        java.util.Map<String, Scorer> latestScorers = new java.util.HashMap<>();
        for (Scorer s : allScorers) {
            Scorer current = latestScorers.get(s.getScorerName());
            if (current == null || s.getScorerVersion() > current.getScorerVersion()) {
                latestScorers.put(s.getScorerName(), s);
            }
        }
        return new ArrayList<>(latestScorers.values());
    }

    public Scorer getScorer(String experimentId, String name, Integer version) throws IOException {
        if (version == null) {
            List<Scorer> versions = listScorerVersions(experimentId, name);
            return versions.stream()
                    .max(java.util.Comparator.comparingInt(Scorer::getScorerVersion))
                    .orElse(null);
        }
        
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Get get = new Get(Bytes.toBytes(experimentId + "_" + name + "_" + version));
            Result result = table.get(get);
            if (result.isEmpty()) return null;
            return mapResultToScorer(result);
        }
    }

    public void deleteScorer(String experimentId, String name, Integer version) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            if (version != null) {
                Delete delete = new Delete(Bytes.toBytes(experimentId + "_" + name + "_" + version));
                table.delete(delete);
            } else {
                List<Scorer> versions = listScorerVersions(experimentId, name);
                for (Scorer s : versions) {
                    Delete delete = new Delete(Bytes.toBytes(experimentId + "_" + name + "_" + s.getScorerVersion()));
                    table.delete(delete);
                }
            }
        }
    }

    private Scorer mapResultToScorer(Result result) {
        String experimentIdStr = Bytes.toString(result.getValue(CF_INFO, COL_EXPERIMENT_ID));
        return Scorer.builder()
                .scorerId(Bytes.toString(result.getValue(CF_INFO, Bytes.toBytes("scorer_id"))))
                .experimentId(Integer.parseInt(experimentIdStr))
                .scorerName(Bytes.toString(result.getValue(CF_INFO, COL_NAME)))
                .scorerVersion(Bytes.toInt(result.getValue(CF_INFO, COL_VERSION)))
                .serializedScorer(Bytes.toString(result.getValue(CF_INFO, COL_SERIALIZED_SCORER)))
                .creationTime(Bytes.toLong(result.getValue(CF_INFO, COL_CREATION_TIME)))
                .build();
    }
}
