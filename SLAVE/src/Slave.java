import java.io.*;
import java.util.HashMap;
import java.util.Map;
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
        //sl.map();
        sl.reduce();
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
            // Print the results
            checkResults(fileOutput);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void reduce(){
        // Get the file number
        String[] items = fileToMap.split("/");
        String numberOfFile = items[items.length-1].split("")[1];
        String fileOutput = rootFolder+"splits/UM"+numberOfFile+".txt";
        String line;
        Map<String, Integer> result = new HashMap<>();

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
                for (String item : items) result.merge(item, 1, Integer::sum);
            }

            // Print the keys to the output file
            for (String key : result.keySet()) out.println(key);

            // Close the handlers
            out.close();
            sc.close();
            br.close();
            fr.close();
            // Print the results
            checkResults(fileOutput);
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Function to print the contents of the file just written
     * @param file the file to print
     */
    private void checkResults(String file) throws FileNotFoundException {

        FileReader fr = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);
        Scanner sc = new Scanner(br);

        while (sc.hasNextLine()) System.out.println(sc.nextLine());
        
        sc.close();
    }
}
