package varga.kirka.repo;
import lombok.extern.slf4j.Slf4j;
import varga.kirka.model.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

@Slf4j
@Repository
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

    @Autowired
    private Connection connection;

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

    public void logBatch(String runId, List<Metric> metrics, List<Param> params, List<RunTag> tags) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
             Table historyTable = connection.getTable(TableName.valueOf(METRIC_HISTORY_TABLE))) {
            Put put = new Put(Bytes.toBytes(runId));
            if (metrics != null) {
                for (Metric m : metrics) {
                    String key = m.getKey();
                    double value = m.getValue();
                    long timestamp = m.getTimestamp();
                    long step = m.getStep();

                    // Update latest value in runs table
                    put.addColumn(CF_METRICS, Bytes.toBytes(key), Bytes.toBytes(value));

                    // Add to history table
                    // Row key: runId + key + timestamp (reversed for scan)
                    byte[] historyRowKey = Bytes.toBytes(runId + "_" + key + "_" + (Long.MAX_VALUE - timestamp));
                    Put historyPut = new Put(historyRowKey);
                    historyPut.addColumn(CF_INFO, Bytes.toBytes("key"), Bytes.toBytes(key));
                    historyPut.addColumn(CF_INFO, Bytes.toBytes("value"), Bytes.toBytes(value));
                    historyPut.addColumn(CF_INFO, Bytes.toBytes("timestamp"), Bytes.toBytes(timestamp));
                    historyPut.addColumn(CF_INFO, Bytes.toBytes("step"), Bytes.toBytes(step));
                    historyTable.put(historyPut);
                }
            }
            if (params != null) {
                for (Param p : params) {
                    put.addColumn(CF_PARAMS, Bytes.toBytes(p.getKey()), Bytes.toBytes(p.getValue()));
                }
            }
            if (tags != null) {
                for (RunTag t : tags) {
                    put.addColumn(CF_TAGS, Bytes.toBytes(t.getKey()), Bytes.toBytes(t.getValue()));
                }
            }
            table.put(put);
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

        byte[] experimentId = result.getValue(CF_INFO, COL_EXPERIMENT_ID);
        byte[] statusBytes = result.getValue(CF_INFO, COL_STATUS);
        byte[] startTime = result.getValue(CF_INFO, COL_START_TIME);
        byte[] endTime = result.getValue(CF_INFO, COL_END_TIME);
        byte[] artifactUri = result.getValue(CF_INFO, COL_ARTIFACT_URI);
        byte[] lifecycleStage = result.getValue(CF_INFO, Bytes.toBytes("lifecycle_stage"));

        RunStatus status = RunStatus.RUNNING;
        if (statusBytes != null) {
            try {
                status = RunStatus.valueOf(Bytes.toString(statusBytes));
            } catch (Exception e) {
                log.warn("Could not parse status: {}", Bytes.toString(statusBytes));
            }
        }

        return Run.builder()
                .info(RunInfo.builder()
                        .runId(runId)
                        .runUuid(runId)
                        .experimentId(experimentId != null ? Bytes.toString(experimentId) : null)
                        .status(status)
                        .startTime(startTime != null ? Bytes.toLong(startTime) : 0L)
                        .endTime(endTime != null ? Bytes.toLong(endTime) : 0L)
                        .artifactUri(artifactUri != null ? Bytes.toString(artifactUri) : null)
                        .lifecycleStage(lifecycleStage != null ? Bytes.toString(lifecycleStage) : "active")
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

    public List<varga.kirka.model.Metric> getMetricHistory(String runId, String metricKey) throws IOException {
        List<varga.kirka.model.Metric> history = new ArrayList<>();
        try (Table table = connection.getTable(TableName.valueOf(METRIC_HISTORY_TABLE))) {
            Scan scan = new Scan();
            // Prefix filter for runId and metricKey
            String prefix = runId + "_" + metricKey + "_";
            scan.setRowPrefixFilter(Bytes.toBytes(prefix));
            
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    history.add(varga.kirka.model.Metric.builder()
                            .key(Bytes.toString(result.getValue(CF_INFO, Bytes.toBytes("key"))))
                            .value(Bytes.toDouble(result.getValue(CF_INFO, Bytes.toBytes("value"))))
                            .timestamp(Bytes.toLong(result.getValue(CF_INFO, Bytes.toBytes("timestamp"))))
                            .step(Bytes.toLong(result.getValue(CF_INFO, Bytes.toBytes("step"))))
                            .build());
                }
            }
        }
        // Result is already sorted by timestamp (reversed) due to row key design
        return history;
    }

    private List<Param> extractParams(Result result) {
        List<Param> list = new ArrayList<>();
        java.util.NavigableMap<byte[], byte[]> map = result.getFamilyMap(CF_PARAMS);
        if (map != null) {
            for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
                list.add(new Param(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue())));
            }
        }
        return list;
    }

    private List<RunTag> extractTags(Result result) {
        List<RunTag> list = new ArrayList<>();
        java.util.NavigableMap<byte[], byte[]> map = result.getFamilyMap(CF_TAGS);
        if (map != null) {
            for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
                list.add(new RunTag(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue())));
            }
        }
        return list;
    }

    private List<Metric> extractMetrics(Result result) {
        List<Metric> map = new ArrayList<>();
        java.util.NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(CF_METRICS);
        if (familyMap != null) {
            for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                map.add(Metric.builder()
                        .key(Bytes.toString(entry.getKey()))
                        .value(Bytes.toDouble(entry.getValue()))
                        .build());
            }
        }
        return map;
    }
}
