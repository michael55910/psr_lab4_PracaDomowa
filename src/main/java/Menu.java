import java.util.Scanner;

public class Menu {
    int selectOperation() {
        System.out.println("Choose operation:");
        System.out.println("[1] Add");
        System.out.println("[2] Update");
        System.out.println("[3] Delete");
        System.out.println("[4] Get by ID");
        System.out.println("[5] Query");
        System.out.println("[6] Process");
        System.out.println("[0] Exit");
        System.out.print("Choice: ");
        Scanner scanner = new Scanner(System.in);
        int choice = scanner.nextInt();
        clearScreen();
        return choice;
    }

    int selectTarget() {
        System.out.println("Choose target:");
        System.out.println("[1] Car");
        System.out.println("[2] RepairBookEntry");
        Scanner scanner = new Scanner(System.in);
        int choice = scanner.nextInt();
        clearScreen();
        return choice;
    }

    private void clearScreen() {
        System.out.print("\033[H\033[2J");
        System.out.flush();
    }
}
