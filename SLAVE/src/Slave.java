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
            this.map();
        } else if (this.opCode == 1 || this.opCode == 2){
            this.reduceKey = args[1];
            this.filesList = new ArrayList<>();
            this.filesList.addAll(Arrays.asList(args).subList(2, args.length));
            this.reduce();
        } else {
        	System.out.println("Error: opCode must be 0/1");
        }
    }

    public int getOpCode() {
		return opCode;
	}

	public static void main(String[] args) throws IOException {

        
        if (args.length == 0) {
        	// [NO ARGS - FOR DEBUG ONLY - COMMENT ABOVE AND UNCOMMENT BELOW]
        	String cmd = "0 /tmp/dgallitelli/splits/S1.txt";
            new Slave(cmd.split(" "));
            // Phase 2 - Slave opCode 1 - Reduce phase 1 [UM --> SM]
            cmd = "1 Car /tmp/dgallitelli/maps/SM1.txt /tmp/dgallitelli/splits/UM1.txt";
            new Slave(cmd.split(" "));
            // Phase 2 - Slave opCode 1 - Reduce phase 2 [SM --> RM]
            cmd = "2 Car /tmp/dgallitelli/maps/SM1.txt /tmp/dgallitelli/reduces/RM1.txt";
            new Slave(cmd.split(" "));
        } else if (args.length < 2){
        	// Wrong number of args - exit
            System.out.println("Not enough args.");
            return;
        } else 
        	new Slave(args);

    }

   	private void map(){
   		// Phase 1 - Slave opCode 0 [S --> UM]
        // System.out.println("[MAP]");
        // Get the file number
        String[] items = fileToMap.split("/");
        String numberOfFile = items[items.length-1].split("")[1];
        String fileOutput = rootFolder+"splits/UM"+numberOfFile+".txt";
        String line;
        Set<String> keys = new HashSet<>();

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
                for (String item : items) {
                	if (item.equals("")) continue;
                	out.println(item+" "+1);
                	keys.add(item);
                }
            }

            // Close the handlers
            sc.close();
            br.close();
            fr.close();
            out.close();
            // Print the keys - they will be read by Master to generate the UMx - keys
            for (String key : keys) System.out.println(key);
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void reduce() throws IOException {
    	String fileSM = this.filesList.get(0);
        // Get the file to write ready
        PrintWriter out;
        // Variables for file read
        FileReader fr;
        BufferedReader br;
        Scanner sc;
        String line;
    	
    	if (this.opCode == 1) {
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
                        out.println(line);
                    }
                }
                sc.close();
                br.close();
                fr.close();
            }
            out.close();
            checkResults(fileSM);
    	} else if (this.opCode == 2) {
            System.out.println("[REDUCE PHASE 2]");
    		// Reduce Step 2 - from SMx to RMx
            String fileRM = this.filesList.get(1);
            fr = new FileReader(fileSM);
            br = new BufferedReader(fr);
            sc = new Scanner(br);
            
            // Actual reduce
            int count = 0;            
            while (sc.hasNextLine()){
                line = sc.nextLine();
                String[] splits = line.split(" ");
                if (splits[0].equals(this.reduceKey)) count++;
            }
            // Print on RM file
    		out = new PrintWriter(new BufferedWriter(new FileWriter(fileRM)));
        	out.write(this.reduceKey+" "+count+"\n");        
            out.close();
            
            sc.close();
            br.close();
            fr.close();
            checkResults(fileRM);
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
    
    @SuppressWarnings("unused")
	private String extractFileType(String path) {
    	String[] outputBits = path.split("/");
    	String fileType = outputBits[outputBits.length-1].substring(0, 2);
    	return fileType;
    }
}
