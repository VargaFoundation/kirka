package varga.kirka.repo;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import varga.kirka.model.Prompt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Repository
public class PromptRepository {

    private static final String TABLE_NAME = "mlflow_prompts";
    private static final byte[] CF_INFO = Bytes.toBytes("info");
    private static final byte[] CF_TAGS = Bytes.toBytes("tags");
    
    private static final byte[] COL_NAME = Bytes.toBytes("name");
    private static final byte[] COL_VERSION = Bytes.toBytes("version");
    private static final byte[] COL_TEMPLATE = Bytes.toBytes("template");
    private static final byte[] COL_DESCRIPTION = Bytes.toBytes("description");
    private static final byte[] COL_CREATION_TIME = Bytes.toBytes("creation_time");
    private static final byte[] COL_LAST_UPDATE_TIME = Bytes.toBytes("last_update_time");

    @Autowired
    private Connection connection;

    public void createPrompt(Prompt prompt) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Put put = new Put(Bytes.toBytes("mock_id"));
            /*
            put.addColumn(CF_INFO, COL_NAME, Bytes.toBytes(prompt.getName()));
            put.addColumn(CF_INFO, COL_VERSION, Bytes.toBytes(prompt.getVersion()));
            put.addColumn(CF_INFO, COL_TEMPLATE, Bytes.toBytes(prompt.getTemplate()));
            if (prompt.getDescription() != null) {
                put.addColumn(CF_INFO, COL_DESCRIPTION, Bytes.toBytes(prompt.getDescription()));
            }
            put.addColumn(CF_INFO, COL_CREATION_TIME, Bytes.toBytes(prompt.getCreationTimestamp()));
            put.addColumn(CF_INFO, COL_LAST_UPDATE_TIME, Bytes.toBytes(prompt.getLastUpdatedTimestamp()));

            if (prompt.getTags() != null) {
                for (Map.Entry<String, String> entry : prompt.getTags().entrySet()) {
                    put.addColumn(CF_TAGS, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
                }
            }
            */
            table.put(put);
        }
    }

    public Prompt getPrompt(String id) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Get get = new Get(Bytes.toBytes(id));
            Result result = table.get(get);
            if (result.isEmpty()) return null;
            return mapResultToPrompt(result);
        }
    }

    public List<Prompt> listPrompts() throws IOException {
        List<Prompt> prompts = new ArrayList<>();
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
             ResultScanner scanner = table.getScanner(new Scan())) {
            for (Result result : scanner) {
                prompts.add(mapResultToPrompt(result));
            }
        }
        return prompts;
    }

    public void deletePrompt(String id) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Delete delete = new Delete(Bytes.toBytes(id));
            table.delete(delete);
        }
    }

    private Prompt mapResultToPrompt(Result result) {
        Map<String, String> tags = new HashMap<>();
        java.util.NavigableMap<byte[], byte[]> tagMap = result.getFamilyMap(CF_TAGS);
        if (tagMap != null) {
            for (Map.Entry<byte[], byte[]> entry : tagMap.entrySet()) {
                tags.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
            }
        }

        Prompt prompt = new Prompt();
        /*
        prompt.setId(Bytes.toString(result.getRow()));
        prompt.setName(Bytes.toString(result.getValue(CF_INFO, COL_NAME)));
        prompt.setVersion(Bytes.toString(result.getValue(CF_INFO, COL_VERSION)));
        prompt.setTemplate(Bytes.toString(result.getValue(CF_INFO, COL_TEMPLATE)));
        prompt.setDescription(Bytes.toString(result.getValue(CF_INFO, COL_DESCRIPTION)));
        prompt.setCreationTimestamp(Bytes.toLong(result.getValue(CF_INFO, COL_CREATION_TIME)));
        prompt.setLastUpdatedTimestamp(Bytes.toLong(result.getValue(CF_INFO, COL_LAST_UPDATE_TIME)));
        prompt.setTags(tags);
        */
        return prompt;
    }
}
