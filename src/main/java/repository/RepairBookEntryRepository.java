package repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import config.Config;
import model.RepairBookEntry;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.*;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.TimestampValue;

import java.sql.Timestamp;
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

    public RepairBookEntry addEntry() {
        RepairBookEntry repairBookEntry = enterEntryInfo();
        save(repairBookEntry);
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
        if (repairBookEntry != null) {
            repairBookEntry.setId(id);
        }
        save(repairBookEntry);
        return repairBookEntry;
    }

    public Collection<RepairBookEntry> getByDate() {
        System.out.print("Podaj datę (dd-MM-yyyy): ");
        Date date = new Date();
        try {
            date = new SimpleDateFormat("dd-MM-yyyy").parse(new Scanner(System.in).next());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        QueryRequest queryRequest = new QueryRequest().
                setStatement("SELECT * FROM " + tableName + " WHERE date = \"" + new TimestampValue(new Timestamp(date.getTime())).getString() + "\"");

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
            }
        } while (!queryRequest.isDone());

        System.out.println(repairBookEntries);
        return repairBookEntries;
    }

    public void MarkAllDone() {
        QueryRequest queryRequest = new QueryRequest().
                setStatement("SELECT * FROM " + tableName);

        int count = 0;
        do {
            QueryResult queryResult = handle.query(queryRequest);
            List<MapValue> results = queryResult.getResults();
            for (MapValue qval : results) {
                if (!qval.getString("description").contains("DONE")) {
                    qval.put("description", "DONE: " + qval.getString("description"));
                    //qval.put("description", "DONE");
                    PutRequest putRequest = new PutRequest().setValue(qval).setTableName(tableName);
                    PutResult putRes = handle.put(putRequest);
                    count++;
                }

            }
        } while (!queryRequest.isDone());
        System.out.println("Modified " + count + " rows");
    }

    private RepairBookEntry enterEntryInfo() {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Car ID: ");
        Long carId = scanner.nextLong();
        if (new CarRepository().getById(carId) == null) {
            System.out.println("Id not found");
            return null;
        }
        System.out.print("Date (dd-MM-yyyy): ");
        Date date = new Date();
        try {
            date = new SimpleDateFormat("dd-MM-yyy").parse(scanner.next());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        System.out.print("Description: ");
        String description = scanner.next();
        return new RepairBookEntry(Math.abs(random.nextLong()), carId, date, description);
    }

    private void save(RepairBookEntry repairBookEntry) {
        if (repairBookEntry != null) {
            TimestampValue timestampValue = new TimestampValue(new Timestamp(repairBookEntry.getDate().getTime()));
            MapValue record = new MapValue();
            record.put("id", repairBookEntry.getId())
                    .put("carId", repairBookEntry.getCarId())
                    .put("date", timestampValue)
                    .put("description", repairBookEntry.getDescription());
            PutRequest putRequest = new PutRequest().setValue(record).setTableName(tableName);
            PutResult putRes = handle.put(putRequest);
            if (putRes.getVersion() != null) {
                System.out.println("Successfully added/updated");
            } else {
                System.out.println("Failed to add/update");
            }
        }
    }
}
