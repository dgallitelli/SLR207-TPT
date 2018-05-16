import java.io.*;
import java.util.Scanner;

public class Slave {

    private String fileToMap;
    private static String rootFolder = "/tmp/dgallitelli/";

    public Slave(String fileToMap) {
        this.fileToMap = fileToMap;
    }

    public static void main(String[] args) {

        if (args.length < 1){
            System.out.println("Not enough args.");
            return;
        }

        Slave sl = new Slave(args[0]);
        sl.map();
    }

    private void map() {
        // Get the file number
        String[] items = fileToMap.split("/");
        String numberOfFile = items[items.length-1].split("")[1];
        String fileOutput = rootFolder+"splits/UM"+numberOfFile+".txt";
        String line;

        // Read the file specified by the parameter
        try {
            FileReader fr = new FileReader(fileToMap);
            BufferedReader br = new BufferedReader(fr);
            Scanner sc = new Scanner(br);
            // Get the file to write ready
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(fileOutput)));

            // Read a line and update the map
            while (sc.hasNextLine()){
                line = sc.nextLine();
                items = line.split(" ");
                for (String item : items) out.println(item + " " + "1");
            }

            // Close the handlers
            out.close();
            sc.close();
            br.close();
            fr.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
