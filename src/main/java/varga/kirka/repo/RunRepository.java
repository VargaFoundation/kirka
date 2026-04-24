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
public class RunRepository {

    private static final String TABLE_NAME = "mlflow_runs";
    private static final String METRIC_HISTORY_TABLE = "mlflow_metric_history";
    private static final byte[] CF_INFO = Bytes.toBytes("info");
    private static final byte[] CF_PARAMS = Bytes.toBytes("params");
    private static final byte[] CF_METRICS = Bytes.toBytes("metrics");
    private static final byte[] CF_TAGS = Bytes.toBytes("tags");

    private static final byte[] COL_EXPERIMENT_ID = Bytes.toBytes("experiment_id");
    private static final byte[] COL_STATUS = Bytes.toBytes("status");
    private static final byte[] COL_START_TIME = Bytes.toBytes("start_time");
    private static final byte[] COL_END_TIME = Bytes.toBytes("end_time");
    private static final byte[] COL_ARTIFACT_URI = Bytes.toBytes("artifact_uri");

    private final Connection connection;

    public void createRun(Run run) throws IOException {
        log.info("HBase: creating run {}", run.getInfo().getRunId());
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Put put = new Put(Bytes.toBytes(run.getInfo().getRunId()));
            put.addColumn(CF_INFO, COL_EXPERIMENT_ID, Bytes.toBytes(run.getInfo().getExperimentId()));
            put.addColumn(CF_INFO, COL_STATUS, Bytes.toBytes(run.getInfo().getStatus().name()));
            put.addColumn(CF_INFO, COL_START_TIME, Bytes.toBytes(run.getInfo().getStartTime()));
            put.addColumn(CF_INFO, COL_ARTIFACT_URI, Bytes.toBytes(run.getInfo().getArtifactUri()));
            put.addColumn(CF_INFO, Bytes.toBytes("lifecycle_stage"), Bytes.toBytes("active"));
            
            if (run.getData() != null && run.getData().getTags() != null) {
                for (RunTag tag : run.getData().getTags()) {
                    put.addColumn(CF_TAGS, Bytes.toBytes(tag.getKey()), Bytes.toBytes(tag.getValue()));
                }
            }
            
            table.put(put);
        }
    }

    public void updateRun(String runId, String status, long endTime) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Put put = new Put(Bytes.toBytes(runId));
            if (status != null) {
                put.addColumn(CF_INFO, COL_STATUS, Bytes.toBytes(status));
            }
            if (endTime > 0) {
                put.addColumn(CF_INFO, COL_END_TIME, Bytes.toBytes(endTime));
            }
            table.put(put);
        }
    }

    public void deleteRun(String runId) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Put put = new Put(Bytes.toBytes(runId));
            put.addColumn(CF_INFO, Bytes.toBytes("lifecycle_stage"), Bytes.toBytes("deleted"));
            table.put(put);
        }
    }

    public void restoreRun(String runId) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Put put = new Put(Bytes.toBytes(runId));
            put.addColumn(CF_INFO, Bytes.toBytes("lifecycle_stage"), Bytes.toBytes("active"));
            table.put(put);
        }
    }

    /** Upper bound on the number of entries (metrics+params+tags) accepted in a single log-batch. */
    private static final int LOG_BATCH_LIMIT = 1000;

    public void logBatch(String runId, List<Metric> metrics, List<Param> params, List<RunTag> tags) throws IOException {
        int entryCount = (metrics != null ? metrics.size() : 0)
                + (params != null ? params.size() : 0)
                + (tags != null ? tags.size() : 0);
        if (entryCount > LOG_BATCH_LIMIT) {
            throw new IllegalArgumentException(
                    "log-batch is capped at " + LOG_BATCH_LIMIT + " entries, received " + entryCount);
        }

        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
             Table historyTable = connection.getTable(TableName.valueOf(METRIC_HISTORY_TABLE))) {
            Put runPut = new Put(Bytes.toBytes(runId));
            List<Put> historyBatch = new ArrayList<>(metrics != null ? metrics.size() : 0);

            if (metrics != null) {
                for (Metric m : metrics) {
                    String key = m.getKey();
                    double value = m.getValue();
                    long timestamp = m.getTimestamp();
                    long step = m.getStep();

                    // Latest value in the runs table
                    runPut.addColumn(CF_METRICS, Bytes.toBytes(key), Bytes.toBytes(value));

                    // History row key: runId + key + reversed timestamp, padded to fixed width so
                    // lexicographic scans return points in descending timestamp order (older last).
                    String paddedRev = String.format("%019d", Long.MAX_VALUE - timestamp);
                    byte[] historyRowKey = Bytes.toBytes(runId + "_" + key + "_" + paddedRev);
                    Put historyPut = new Put(historyRowKey);
                    historyPut.addColumn(CF_INFO, COL_HISTORY_KEY, Bytes.toBytes(key));
                    historyPut.addColumn(CF_INFO, COL_HISTORY_VALUE, Bytes.toBytes(value));
                    historyPut.addColumn(CF_INFO, COL_HISTORY_TIMESTAMP, Bytes.toBytes(timestamp));
                    historyPut.addColumn(CF_INFO, COL_HISTORY_STEP, Bytes.toBytes(step));
                    historyBatch.add(historyPut);
                }
            }
            if (params != null) {
                for (Param p : params) {
                    runPut.addColumn(CF_PARAMS, Bytes.toBytes(p.getKey()), Bytes.toBytes(p.getValue()));
                }
            }
            if (tags != null) {
                for (RunTag t : tags) {
                    runPut.addColumn(CF_TAGS, Bytes.toBytes(t.getKey()), Bytes.toBytes(t.getValue()));
                }
            }

            if (!runPut.isEmpty()) {
                table.put(runPut);
            }
            if (!historyBatch.isEmpty()) {
                historyTable.put(historyBatch);
            }
        }
    }

    public void setTag(String runId, String key, String value) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Put put = new Put(Bytes.toBytes(runId));
            put.addColumn(CF_TAGS, Bytes.toBytes(key), Bytes.toBytes(value));
            table.put(put);
        }
    }

    public void deleteTag(String runId, String key) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Delete delete = new Delete(Bytes.toBytes(runId));
            delete.addColumns(CF_TAGS, Bytes.toBytes(key));
            table.delete(delete);
        }
    }

    private static final byte[] CF_INPUTS = Bytes.toBytes("inputs");
    private static final byte[] COL_LOGGED_MODELS = Bytes.toBytes("mlflow.log-model.history");

    /**
     * Records dataset inputs for a run (MLFlow 2.4+ log-inputs). Each serialized dataset is
     * stored as a JSON blob keyed by a short digest so re-logging the same dataset is idempotent.
     */
    public void logInputs(String runId, List<String> serializedInputs) throws IOException {
        if (serializedInputs == null || serializedInputs.isEmpty()) return;
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Put put = new Put(Bytes.toBytes(runId));
            for (String json : serializedInputs) {
                String digest = Integer.toHexString(json.hashCode());
                put.addColumn(CF_INPUTS, Bytes.toBytes(digest), Bytes.toBytes(json));
            }
            table.put(put);
        }
    }

    /**
     * Appends a logged-model entry for a run (MLFlow log_model). Multiple models per run are
     * allowed; history is kept as a newline-separated blob so callers append without re-reading.
     */
    public void logModel(String runId, String modelJson) throws IOException {
        if (modelJson == null || modelJson.isBlank()) return;
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Get get = new Get(Bytes.toBytes(runId));
            get.addColumn(CF_INFO, COL_LOGGED_MODELS);
            Result existing = table.get(get);
            String prev = HBaseResults.getStringOrDefault(existing, CF_INFO, COL_LOGGED_MODELS, "");
            String next = prev.isEmpty() ? modelJson : prev + "\n" + modelJson;
            Put put = new Put(Bytes.toBytes(runId));
            put.addColumn(CF_INFO, COL_LOGGED_MODELS, Bytes.toBytes(next));
            table.put(put);
        }
    }

    public List<Run> searchRuns(List<String> experimentIds, String filter, String runViewType) throws IOException {
        List<Run> runs = new ArrayList<>();
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Scan scan = new Scan();
            // Simplification: on filtre par experiment_id si fourni
            if (experimentIds != null && !experimentIds.isEmpty()) {
                // HBase FilterList pour multiple experiment_ids
                org.apache.hadoop.hbase.filter.FilterList filterList = new org.apache.hadoop.hbase.filter.FilterList(org.apache.hadoop.hbase.filter.FilterList.Operator.MUST_PASS_ONE);
                for (String id : experimentIds) {
                    filterList.addFilter(new org.apache.hadoop.hbase.filter.SingleColumnValueFilter(
                            CF_INFO, COL_EXPERIMENT_ID, org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.EQUAL, Bytes.toBytes(id)));
                }
                scan.setFilter(filterList);
            }
            
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    runs.add(mapResultToRun(result));
                }
            }
        }
        return runs;
    }

    private Run mapResultToRun(Result result) {
        String runId = Bytes.toString(result.getRow());
        
        List<Metric> metrics = new ArrayList<>();
        java.util.NavigableMap<byte[], byte[]> metricsMap = result.getFamilyMap(CF_METRICS);
        if (metricsMap != null) {
            for (Map.Entry<byte[], byte[]> entry : metricsMap.entrySet()) {
                metrics.add(Metric.builder()
                        .key(Bytes.toString(entry.getKey()))
                        .value(Bytes.toDouble(entry.getValue()))
                        .build());
            }
        }

        List<Param> params = new ArrayList<>();
        java.util.NavigableMap<byte[], byte[]> paramsMap = result.getFamilyMap(CF_PARAMS);
        if (paramsMap != null) {
            for (Map.Entry<byte[], byte[]> entry : paramsMap.entrySet()) {
                params.add(Param.builder()
                        .key(Bytes.toString(entry.getKey()))
                        .value(Bytes.toString(entry.getValue()))
                        .build());
            }
        }

        List<RunTag> tags = new ArrayList<>();
        java.util.NavigableMap<byte[], byte[]> tagsMap = result.getFamilyMap(CF_TAGS);
        if (tagsMap != null) {
            for (Map.Entry<byte[], byte[]> entry : tagsMap.entrySet()) {
                tags.add(RunTag.builder()
                        .key(Bytes.toString(entry.getKey()))
                        .value(Bytes.toString(entry.getValue()))
                        .build());
            }
        }

        String statusString = HBaseResults.getStringOrNull(result, CF_INFO, COL_STATUS);
        RunStatus status = RunStatus.RUNNING;
        if (statusString != null) {
            try {
                status = RunStatus.valueOf(statusString);
            } catch (IllegalArgumentException e) {
                log.warn("Could not parse run status: {}", statusString);
            }
        }

        return Run.builder()
                .info(RunInfo.builder()
                        .runId(runId)
                        .runUuid(runId)
                        .experimentId(HBaseResults.getStringOrNull(result, CF_INFO, COL_EXPERIMENT_ID))
                        .status(status)
                        .startTime(HBaseResults.getLongOrDefault(result, CF_INFO, COL_START_TIME, 0L))
                        .endTime(HBaseResults.getLongOrDefault(result, CF_INFO, COL_END_TIME, 0L))
                        .artifactUri(HBaseResults.getStringOrNull(result, CF_INFO, COL_ARTIFACT_URI))
                        .lifecycleStage(HBaseResults.getStringOrDefault(result, CF_INFO, Bytes.toBytes("lifecycle_stage"), "active"))
                        .build())
                .data(RunData.builder()
                        .metrics(metrics)
                        .params(params)
                        .tags(tags)
                        .build())
                .build();
    }

    public Run getRun(String runId) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Get get = new Get(Bytes.toBytes(runId));
            Result result = table.get(get);
            if (result.isEmpty()) return null;

            return mapResultToRun(result);
        }
    }

    private static final byte[] COL_HISTORY_KEY = Bytes.toBytes("key");
    private static final byte[] COL_HISTORY_VALUE = Bytes.toBytes("value");
    private static final byte[] COL_HISTORY_TIMESTAMP = Bytes.toBytes("timestamp");
    private static final byte[] COL_HISTORY_STEP = Bytes.toBytes("step");

    public List<Metric> getMetricHistory(String runId, String metricKey) throws IOException {
        List<Metric> history = new ArrayList<>();
        try (Table table = connection.getTable(TableName.valueOf(METRIC_HISTORY_TABLE))) {
            Scan scan = new Scan();
            String prefix = runId + "_" + metricKey + "_";
            scan.setRowPrefixFilter(Bytes.toBytes(prefix));
            scan.setCaching(100);

            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    history.add(Metric.builder()
                            .key(HBaseResults.getStringOrDefault(result, CF_INFO, COL_HISTORY_KEY, metricKey))
                            .value(HBaseResults.getDoubleOrDefault(result, CF_INFO, COL_HISTORY_VALUE, 0d))
                            .timestamp(HBaseResults.getLongOrDefault(result, CF_INFO, COL_HISTORY_TIMESTAMP, 0L))
                            .step(HBaseResults.getLongOrDefault(result, CF_INFO, COL_HISTORY_STEP, 0L))
                            .build());
                }
            }
        }
        return history;
    }
}
