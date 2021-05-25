import config.Config;
import model.Car;
import model.RepairBookEntry;
import oracle.nosql.driver.NoSQLHandle;
import org.jetbrains.annotations.NotNull;
import repository.CarRepository;
import repository.RepairBookEntryRepository;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
       /* ClientConfig clientConfig = null;
        try {
            clientConfig = HConfig.getClientConfig();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        HazelcastInstance client = HazelcastClient.newHazelcastClient( clientConfig );
        IMap<Long, Car> cars = client.getMap("cars");
        IMap<Long, RepairBookEntry> repairBookEntries = client.getMap("repairBookEntry");*/

        //NoSQLHandle handle = Config.getHandle();

        CarRepository carRepository = new CarRepository();
        RepairBookEntryRepository repairBookEntryRepository = new RepairBookEntryRepository();

        //MENU
        Menu menu = new Menu();
        while (true) {
            int operation = menu.selectOperation();
            if (operation == 0) {
                break;
            }
            int target = menu.selectTarget();
            switch (operation) {
                case 1:
                    switch (target) {
                        case 1 -> carRepository.addCar();
                        case 2 -> repairBookEntryRepository.addEntry();
                    }
                    break;
                case 2:
                    switch (target) {
                        case 1 -> carRepository.updateById(getId());
                        case 2 -> repairBookEntryRepository.updateById(getId());
                    }
                    break;
                case 3:
                    switch (target) {
                        case 1 -> carRepository.deleteById(getId());
                        case 2 -> repairBookEntryRepository.deleteById(getId());
                    }
                    break;
                case 4:
                    switch (target) {
                        case 1 -> carRepository.getById(getId());
                        case 2 -> repairBookEntryRepository.getById(getId());
                    }
                    break;
                case 5:
                    Scanner scanner = new Scanner(System.in);
                    switch (target) {
                        case 1:
                            System.out.print("Podaj model: ");
                            carRepository.getByModel(scanner.nextLine());
                        case 2:
                            System.out.print("Podaj datę (dd-MM-yyy): ");
                            try {
                                repairBookEntryRepository.getByDate(new SimpleDateFormat("dd-MM-yyyy").parse(scanner.nextLine()));
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                    }
                    break;
                case 6:
                    switch (target) {
                        case 2 -> repairBookEntryRepository.MarkAllDone();
                    }
                    break;
                default:
                    System.out.println("Błędny wybór!");
                    return;
            }
        }
        
    }

    @NotNull
    private static Long getId() {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Podaj id: ");
        return scanner.nextLong();
    }

}
