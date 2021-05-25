package repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import config.Config;
import model.Car;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.*;
import oracle.nosql.driver.values.MapValue;

import java.util.*;

public class CarRepository {

    private final Random random = new Random(System.currentTimeMillis());
    private final static String tableName = "cars";
    NoSQLHandle handle;
    private final static ObjectMapper objectMapper = new ObjectMapper();

    public CarRepository() {

        final String createTableDDL = "CREATE TABLE IF NOT EXISTS " + tableName +
                " (id LONG, idPlate STRING, brand STRING, " +
                "model STRING, manufactureYear INTEGER, " +
                "PRIMARY KEY(id))";

        handle = Config.getHandle();
        TableLimits limits = new TableLimits(200, 100, 5);
        TableRequest treq = new TableRequest().setStatement(createTableDDL).setTableLimits(limits);

        TableResult tres = handle.tableRequest(treq);
        tres.waitForCompletion(handle, 60000, 1000);

        treq = new TableRequest().setStatement("CREATE INDEX IF NOT EXISTS modelIdx ON + " + tableName + "(model) ");
        handle.tableRequest(treq);
        tres.waitForCompletion(handle, 60000, 1000);

    }

    public Car addCar() {
        Car car = enterCarInfo();
        /*MapValue value = new MapValue()
                .put("idPlate", car.getIdPlate())
                .put("brand", car.getBrand())
                .put("model", car.getModel())
                .put("manufactureYear", car.getManufactureYear());

        PutRequest putRequest = new PutRequest().setValue(value).setTableName(tableName);*/

        UniversalRepository.save(car, tableName);

        System.out.println(car);
        return car;
    }

/*    private void saveCar(Car car) {
        PutRequest putRequest = null;
        try {
            putRequest = new PutRequest().setValueFromJson(objectMapper.writeValueAsString(car), null).setTableName(tableName);
        } catch (JsonProcessingException e) {
            System.out.println("JSON processing error");
            e.printStackTrace();
        }
        PutResult putRes = handle.put(putRequest);
        if (putRes.getVersion() != null) {
            System.out.println("Successfully added/updated car");
        } else {
            System.out.println("Failed to add/update car");
        }
    }*/

/*    public void deleteById(Long id) {
        *//* identify the row to delete *//*
        MapValue delKey = new MapValue().put("id", id);

        *//* construct the DeleteRequest *//*
        DeleteRequest delRequest = new DeleteRequest().setKey(delKey).setTableName(tableName);

        *//* Use the NoSQL handle to execute the delete request *//*
        DeleteResult del = handle.delete(delRequest);

        *//* on success DeleteResult.getSuccess() returns true *//*
        if (del.getSuccess()) {
            System.out.println("Successfully removed car");
        } else {
            System.out.println("Id not found");
        }
    }*/
    public void deleteById(Long id) {
        UniversalRepository.deleteById(id, tableName);
    }

    /*public Car getById(Long id) {

        Car car = null;

        MapValue key = new MapValue().put("id", id);
        GetRequest getRequest = new GetRequest().setKey(key).setTableName(tableName);
        GetResult getRes = handle.get(getRequest);

        MapValue res = getRes.getValue();
        if (res != null) {
            System.out.println(res);
            try {
                car = objectMapper.readValue(res.toJson(), Car.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Failed to find");
        }

        System.out.println(car);
        return car;
    }*/

    public Car getById(Long id) {
        return UniversalRepository.getById(id, tableName, Car.class);
    }

    public Car updateById(Long id) {

        Car car = enterCarInfo();

        MapValue key = new MapValue().put("id", id);
        GetRequest getRequest = new GetRequest().setKey(key).setTableName(tableName);
        GetResult getRes = handle.get(getRequest);
        if (getRes.getValue() != null) {
            car.setId(id);
            UniversalRepository.save(car, tableName);
        } else {
            System.out.println("Id not found");
        }

        return car;
    }

    public Collection<Car> getByModel(String model) {

        QueryRequest queryRequest = new QueryRequest().
                setStatement("SELECT * FROM " + tableName + " WHERE model = \"" + model + "\"");

        /* Queries can return partial results. It is necessary to loop,
         * reissuing the request until it is "done"
         */

        List<Car> cars = new ArrayList<>();
        do {
            QueryResult queryResult = handle.query(queryRequest);

            /* process current set of results */
            List<MapValue> results = queryResult.getResults();
            for (MapValue qval : results) {
                try {
                    cars.add(objectMapper.readValue(qval.toJson(), Car.class));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                //handle result
            }
        } while (!queryRequest.isDone());

        System.out.println(cars);
        return cars;
    }

    private Car enterCarInfo() {
        Scanner scanner = new Scanner(System.in);
        System.out.print("ID Plate: ");
        String idPlate = scanner.nextLine();
        System.out.print("Brand: ");
        String brand = scanner.nextLine();
        System.out.print("Model: ");
        String model = scanner.nextLine();
        System.out.print("ManufactureYear: ");
        int manufactureYear = scanner.nextInt();
        return new Car(random.nextLong(), idPlate, brand, model, manufactureYear);
    }
}
