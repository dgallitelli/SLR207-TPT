import java.io.*;
import java.util.*;

public class Slave {

    private int opCode;
    private String fileToMap;
    private List<String> filesList;
    private String reduceKey;
    private static String rootFolder = "/tmp/dgallitelli/";

    public Slave(String[] args) throws IOException {
        this.opCode = Integer.parseInt(args[0]);
        if (this.opCode == 0){
            // Mode mapping
            this.fileToMap = args[1];
            this.map2();
        } else if (this.opCode == 1){
        	if (extractFileType(args[2]).compareTo("SM")==0) {
        		// Reduce 
                this.reduceKey = args[1];
                this.filesList = new ArrayList<>();
                this.filesList.addAll(Arrays.asList(args).subList(2, args.length));
        	} else {
        		// Reduce 2
                this.filesList = new ArrayList<>();
                this.filesList.addAll(Arrays.asList(args).subList(1, args.length));
        	}
            this.reduce();
        } else {
        	System.out.println("Error: opCode must be 0/1");
        }
    }

    public int getOpCode() {
		return opCode;
	}

	public static void main(String[] args) throws IOException {

        if (args.length < 2){
            System.out.println("Not enough args.");
            return;
        }

        new Slave(args);

        // [FOR DEBUG ONLY - COMMENT ABOVE AND UNCOMMENT BELOW]

        /*
        // Phase 1 - Slave opCode 0 [S --> UM]
        String cmd = "0 /tmp/dgallitelli/splits/S1.txt";
        new Slave(cmd.split(" "));
        // Phase 2 - Slave opCode 1 - Reduce phase 1 [UM --> SM]
        cmd = "1 Car /tmp/dgallitelli/maps/SM1.txt /tmp/dgallitelli/splits/UM1.txt";
        new Slave(cmd.split(" "));
        // Phase 2 - Slave opCode 1 - Reduce phase 2 [SM --> RM]
        cmd = "1 /tmp/dgallitelli/maps/SM1.txt /tmp/dgallitelli/reduces/RM1.txt";
        new Slave(cmd.split(" "));
        */
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
	    // checkResults(fileOutput);
    }

   	private void map2(){
        System.out.println("[MAP]");
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
            sc.close();
            br.close();
            fr.close();
            out.close();
            // Print the results
            // checkResults(fileOutput);
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void reduce() throws IOException {
    	// Is it step 1 or 2 ? Check if this.outputFile is a SM or RM
    	String fileType = extractFileType(this.filesList.get(1));
    	String fileSM = this.filesList.get(0);
        // Get the file to write ready
        PrintWriter out;
        // Variables for file read
        FileReader fr;
        BufferedReader br;
        Scanner sc;
        String line;
    	
    	if (fileType.equals("UM")) {
    	    System.out.println("[REDUCE PHASE 1]");
    		// Reduce step 1 - From multiple UMx to one SMx
    		out = new PrintWriter(new BufferedWriter(new FileWriter(fileSM)));
            // For every file in UMFiles
            for (String UM : this.filesList){
                // If on the first element (SM file), skip
            	if (UM.compareTo(fileSM) == 0) continue;
            	// Set up read handlers
                fr = new FileReader(UM);
                br = new BufferedReader(fr);
                sc = new Scanner(br);
                // Read a line and update the SM file
                while (sc.hasNextLine()){
                    line = sc.nextLine();
                    if (this.reduceKey.equals(line.split(" ")[0])){
                        out.write(line+" 1\n");
                    }
                }
                sc.close();
                br.close();
                fr.close();
            }
            out.close();
            checkResults(fileSM);
    	} else {
            System.out.println("[REDUCE PHASE 2]");
    		// Reduce Step 2 - from SMx to RMx
    		out = new PrintWriter(new BufferedWriter(new FileWriter(this.filesList.get(1))));
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
            	out.write(key+" "+myMap.get(key)+"\n");
            sc.close();
            br.close();
            fr.close();            
            out.close();
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
    
    private String extractFileType(String path) {
    	String[] outputBits = path.split("/");
    	String fileType = outputBits[outputBits.length-1].substring(0, 2);
    	return fileType;
    }
}
