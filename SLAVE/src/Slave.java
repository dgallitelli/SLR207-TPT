import java.io.*;
import java.util.*;

public class Slave {

    private int opCode;
    private String fileToMap;
    private List<String> filesList;
    private String reduceKey;
    private static String rootFolder = "/tmp/dgallitelli/";

    public Slave(String[] args) {
        this.opCode = Integer.parseInt(args[0]);
        if (this.opCode == 0){
            // Mode mapping
            this.fileToMap = args[1];
        } else if (this.opCode == 1){
            this.reduceKey = args[1];
            this.filesList = new ArrayList<>();
            for (int i = 2; i<args.length; i++)
                this.filesList.add(args[i]);
        } else {
        	System.out.println("Error: opCode must be 0/1");
        }
    }

    public static void main(String[] args) {

        if (args.length < 2){
            System.out.println("Not enough args.");
            return;
        }

        Slave sl = new Slave(args);
        try {
            //sl.map();
            //sl.map2();
            sl.reduce();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unused")
	private void map() throws IOException{
        // Get the file number
        String[] items = fileToMap.split("/");
        String numberOfFile = items[items.length-1].split("")[1];
        String fileOutput = rootFolder+"splits/UM"+numberOfFile+".txt";
        String line;

        // Read the file specified by the parameter
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
    }

    @SuppressWarnings("unused")
	private void map2(){
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

    private void reduce() throws IOException {
    	
    	// Is it step 1 or 2 ? Check if this.outputFile is a SM or RM
    	String[] outputBits = this.filesList.get(1).split("/");
    	String fileType = outputBits[outputBits.length-1].substring(0, 2);
        // Get the file to write ready
        PrintWriter out;
        // Variables for file read
        FileReader fr;
        BufferedReader br;
        Scanner sc;
        String line;
    	
    	if (fileType.equals("UM")) {
    		// Reduce step 1
    		out = new PrintWriter(new BufferedWriter(new FileWriter(this.filesList.get(0))));
            // For every file in UMFiles
            for (String UM : this.filesList){
            	if (this.filesList.indexOf(UM) == 0) continue;
                fr = new FileReader(UM);
                br = new BufferedReader(fr);
                sc = new Scanner(br);
                // Read a line and update the SM file
                while (sc.hasNextLine()){
                    line = sc.nextLine();
                    if (this.reduceKey.equals(line.split(" ")[0])){
                        // Write on output file a new line
                        out.write(line+"\n");
                    }
                }
                sc.close();
                br.close();
                fr.close();
            }
    	} else {
    		// Reduce Step 2
    		out = new PrintWriter(new BufferedWriter(new FileWriter(this.filesList.get(0))));
            fr = new FileReader(this.filesList.get(0));
            br = new BufferedReader(fr);
            sc = new Scanner(br);
            // Read and reduce in hashmap
            Map<String,Integer> myMap = new HashMap<>();
            while (sc.hasNextLine()){
                line = sc.nextLine();
                String[] splits = line.split(" ");
                myMap.merge(splits[0], Integer.parseInt(splits[1]), Integer::sum);
            }
            for (String key : myMap.keySet())
            	out.write(key+" "+myMap.get(key).toString()+"\n");
            sc.close();
            br.close();
            fr.close();
        }            
        out.close();
        
        // From SMx to RMx
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
