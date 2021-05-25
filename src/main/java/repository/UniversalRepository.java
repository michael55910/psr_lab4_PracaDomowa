package repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import config.Config;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.*;
import oracle.nosql.driver.values.MapValue;

public class UniversalRepository {

    private static final NoSQLHandle handle = Config.getHandle();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static <T> void save(T obj, String tableName) {
        PutRequest putRequest = null;
        try {
            putRequest = new PutRequest().setValueFromJson(objectMapper.writeValueAsString(obj), null).setTableName(tableName);
        } catch (JsonProcessingException e) {
            System.out.println("JSON processing error");
            e.printStackTrace();
        }
        PutResult putRes = handle.put(putRequest);
        if (putRes.getVersion() != null) {
            System.out.println("Successfully added/updated");
        } else {
            System.out.println("Failed to add/update");
        }
    }

    static void deleteById(Long id, String tableName) {
        MapValue delKey = new MapValue().put("id", id);
        DeleteRequest delRequest = new DeleteRequest().setKey(delKey).setTableName(tableName);
        DeleteResult del = handle.delete(delRequest);
        if (del.getSuccess()) {
            System.out.println("Successfully removed");
        } else {
            System.out.println("Id not found");
        }
    }

    static <T> T getById(Long id, String tableName, Class<T> contentClass) {

        T obj = null;

        MapValue key = new MapValue().put("id", id);
        GetRequest getRequest = new GetRequest().setKey(key).setTableName(tableName);
        GetResult getRes = handle.get(getRequest);

        MapValue res = getRes.getValue();
        if (res != null) {
            //System.out.println(res);
            try {
                obj = objectMapper.readValue(res.toJson(), contentClass);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Failed to find");
        }

        System.out.println(obj);
        return obj;
    }

}
