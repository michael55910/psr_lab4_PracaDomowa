package repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import config.Config;
import model.Car;
import model.RepairBookEntry;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.*;
import oracle.nosql.driver.values.MapValue;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class RepairBookEntryRepository {


    private final Random random = new Random(System.currentTimeMillis());
    private final static String tableName = "repairBookEntries";
    NoSQLHandle handle;
    private final static ObjectMapper objectMapper = new ObjectMapper();

    public RepairBookEntryRepository() {

        final String createTableDDL = "CREATE TABLE IF NOT EXISTS " + tableName +
                "(id LONG, carId LONG, date TIMESTAMP(6), description STRING, PRIMARY KEY(id))";

        handle = Config.getHandle();
        TableLimits limits = new TableLimits(200, 100, 5);
        TableRequest treq = new TableRequest().setStatement(createTableDDL).setTableLimits(limits);

        TableResult tres = handle.tableRequest(treq);
        tres.waitForCompletion(handle, 60000, 1000);
    }

    /*private void saveRepairBookEntry(RepairBookEntry repairBookEntry) {
        PutRequest putRequest = null;
        try {
            putRequest = new PutRequest().setValueFromJson(objectMapper.writeValueAsString(repairBookEntry), null).setTableName(tableName);
        } catch (JsonProcessingException e) {
            System.out.println("JSON processing error");
            e.printStackTrace();
        }
        PutResult putRes = handle.put(putRequest);
        if (putRes.getVersion() != null) {
            System.out.println("Successfully added/updated repairBookEntry");
        } else {
            System.out.println("Failed to add/update repairBookEntry");
        }
    }*/

    public RepairBookEntry addEntry() {
        RepairBookEntry repairBookEntry = enterEntryInfo();

        UniversalRepository.save(repairBookEntry, tableName);

        System.out.println(repairBookEntry);
        return repairBookEntry;
    }

    public void deleteById(Long id) {
        UniversalRepository.deleteById(id, tableName);
    }

    public RepairBookEntry getById(Long id) {
        return UniversalRepository.getById(id, tableName, RepairBookEntry.class);
    }

    public RepairBookEntry updateById(Long id) {
        RepairBookEntry repairBookEntry = enterEntryInfo();

        MapValue key = new MapValue().put("id", id);
        GetRequest getRequest = new GetRequest().setKey(key).setTableName(tableName);
        GetResult getRes = handle.get(getRequest);
        if (getRes.getValue() != null) {
            repairBookEntry.setId(id);
            UniversalRepository.save(repairBookEntry, tableName);
        } else {
            System.out.println("Id not found");
        }

        return repairBookEntry;
    }

    public Collection<RepairBookEntry> getByDate(Date date) {
        QueryRequest queryRequest = new QueryRequest().
                setStatement("SELECT * FROM " + tableName + " WHERE date = \"" + date + "\"");

        /* Queries can return partial results. It is necessary to loop,
         * reissuing the request until it is "done"
         */

        List<RepairBookEntry> repairBookEntries = new ArrayList<>();
        do {
            QueryResult queryResult = handle.query(queryRequest);

            /* process current set of results */
            List<MapValue> results = queryResult.getResults();
            for (MapValue qval : results) {
                try {
                    repairBookEntries.add(objectMapper.readValue(qval.toJson(), RepairBookEntry.class));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                //handle result
            }
        } while (!queryRequest.isDone());

        System.out.println(repairBookEntries);
        return repairBookEntries;
    }

    public void MarkAllDone() {
        QueryRequest queryRequest = new QueryRequest().
                setStatement("SELECT * FROM " + tableName + " WHERE description NOT LIKE \"%DONE%\"");

        do {
            QueryResult queryResult = handle.query(queryRequest);
            List<MapValue> results = queryResult.getResults();
            for (MapValue qval : results) {
                qval.put("description", qval.getString("description") + " DONE");
                GetRequest getRequest = new GetRequest().setKey(qval).setTableName(tableName);
                GetResult getRes = handle.get(getRequest);
                getRes.getValue();
            }
        } while (!queryRequest.isDone());
    }

    private RepairBookEntry enterEntryInfo() {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Car ID: ");
        Long carId = scanner.nextLong();
        Car car = null;
        car = new CarRepository().getById(carId);
        if (car != null) {
            System.out.println(car);
        } else {
            System.out.println("Id not found");
        }
        System.out.print("Date: ");
        Date date = null;
        try {
            date = new SimpleDateFormat("dd-MM-yyyy").parse(scanner.nextLine());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        System.out.print("Description: ");
        String description = scanner.nextLine();
        return new RepairBookEntry(random.nextLong(), car, date, description);
    }
}
