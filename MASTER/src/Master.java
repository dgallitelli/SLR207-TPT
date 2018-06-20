import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Master {
	
	private String classroom;
	private int firstPC;
	private String thisPC;
    private static String userPrefix = "dgallitelli@";
    private static String domain = ".enst.fr";
    private static String splitsPath = "/tmp/dgallitelli/splits/";
    private static String mapsPath = "/tmp/dgallitelli/maps/";
    private static String reducesPath = "/tmp/dgallitelli/reduces/";
    private static String slavePath = "/tmp/dgallitelli/Slave.jar";

    private Master() throws Exception {
			setupFirstPC();
    }

    public static void main(String[] args){
    	try {
    		String initFilePath;
    		initFilePath = args.length != 0 ? args[0] : "";
    		// initFilePath = "/cal/homes/dgallitelli/Downloads/forestier_mayotte.txt";
    		// initFilePath = "/cal/homes/dgallitelli/Downloads/energie.txt";
    		// initFilePath = "/cal/homes/dgallitelli/Downloads/shakespeare";
			InitFiles ifs = new InitFiles(initFilePath);
	        Master ms = new Master();
	        ms.newFoo(ifs.getFiles().keySet().size());
	        return;
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
    }
    
    @SuppressWarnings({ "resource", "unused" })
	private void pause() {
		System.out.println("Press Any Key To Continue...");
		new java.util.Scanner(System.in).nextLine();
    }
    
    private void setupFirstPC() throws Exception {
    	ProcessBuilder pb = new ProcessBuilder("hostname");
    	Process p = pb.start();
    	p.waitFor();
    	
    	BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String[] machineInfo = input.readLine().split("-");
        this.classroom = machineInfo[0];
        this.firstPC = Integer.parseInt(machineInfo[1]);
    }

    private void newFoo(int nMachines) throws Exception {

        BufferedReader input;
        String l;
        
        // Define thisPC
        this.thisPC = new StringBuilder("").append(this.classroom).append("-").append(firstPC).toString();

        // Define the target machines
        Map<String, Integer> machinesIDMap = new HashMap<>();
        for (int j = 0; j < nMachines; j++)
        	machinesIDMap.put(classroom+"-"+(firstPC+j), j);
        
        // Dictionary UMx-Machines
        Map<String, String> machinesUMxMap = new HashMap<>();
        for (int j = 0; j < nMachines; j++)
        	machinesUMxMap.put("UM"+j, new String(classroom+"-"+(firstPC+j)));

        // Define the MR map
        Map<String, List<String>> keysUMxMap = new HashMap<>();
        
        // Deploy Slave and files
        new Deploy(this.thisPC, machinesIDMap);
        
        // Get machines
    	String[] machines = machinesIDMap.keySet().toArray(new String[machinesIDMap.keySet().size()]);
    	
    	// ###########################################
        // [RUN MULTIPLE SLAVES IN PARALLEL, MAP MODE]
    	// ###########################################
    	
        ProcessBuilder[] mappersProcessBuilder = new ProcessBuilder[machines.length];
        Process[] mappersProcess = new Process[machines.length];
        for (int j = 0; j < machines.length; j++) {
			mappersProcessBuilder[j] = new ProcessBuilder("ssh", machines[j], "java -jar "+slavePath+" 0 "+splitsPath+"S"+machinesIDMap.get(machines[j])+".txt");
        	mappersProcess[j] = mappersProcessBuilder[j].start();
        	System.out.println("[OK] Launched slave.jar MAP MODE on machine " + machines[j]);
        }
        // Wait for their completion and read the outputs
        for (int j = 0; j < machines.length; j++) mappersProcess[j].waitFor();
        // Receive the outputs of the mappers
        for (int j = 0; j < machines.length; j++) {
            input = new BufferedReader(new InputStreamReader(mappersProcess[j].getInputStream()));
            while ((l = input.readLine()) != null) {
                if (keysUMxMap.containsKey(l)) {
                    keysUMxMap.get(l).add("UM" + machinesIDMap.get(machines[j]));
                } else {
                    List<String> newList = new ArrayList<>();
                    newList.add("UM" + machinesIDMap.get(machines[j]));
                    keysUMxMap.put(l, newList);
                }
            }
        }
        
        // ###########################################

        // Print the results
        for (String r : keysUMxMap.keySet())
        	System.out.println(r + " - <" + keysUMxMap.get(r).toString() + ">");

        

        
        // ###########################################
        // [PREPARE FOR SHUFFLE PHASE]
        // For every key, find an available machine
        // To be used as a reducer
        // ###########################################
        
    	String[] keys = keysUMxMap.keySet().toArray(new String[keysUMxMap.keySet().size()]);	// Obtain the keys
    	int nReducers = nMachines < keys.length ? nMachines : keys.length;						// Choose n_reducers
    	Map<String, List<String>> machineKeysMap = new HashMap<>();	 							// Map for machine-keys
        Map<String, Integer> finalMap = new HashMap<>();										// FinalMap for results
    	
        // Fill the keys-machine map
    	for (int i = 0; i < keys.length; i++) {
        	String reducekey = keys[i];
        	String reducerID = machines[i%nReducers];
        	if (!machineKeysMap.containsKey(reducerID)) machineKeysMap.put(reducerID, new ArrayList<>());
        	machineKeysMap.get(reducerID).add(reducekey);
        }
        
        System.out.println(machineKeysMap.toString());
        
        // ###########################################
    	// [SHUFFLE PHASE]
        // ###########################################
        
        for (String m : machineKeysMap.keySet()) {
        	// For every Reducer: shuffle, reduce1, reduce2
            String reducerFull = String.format("%s%s%s", userPrefix, m, domain);
        	// Get the keys to be processed by m
        	List<String> mKeys = machineKeysMap.get(m);
        	// For every key:
        	for (String key: mKeys) {
        		// Get the files containing this key
        		List<String> mappers = keysUMxMap.get(key);
        		List<ProcessBuilder> listPBs = new ArrayList<>();
        		List<Process> listPs = new ArrayList<>();
                
    	        for (String mapperFile : mappers) {
    	        	// Get mappers info
    	        	String mapper = machinesUMxMap.get(mapperFile);
    	            int mapperID = Integer.parseInt(mapperFile.split("M")[1]);
    	            String mapperFull = String.format("%s%s%s", userPrefix, mapper, domain);
            		// Optimization: check if file is already on reducerFull - avoid unnecessary shuffles
            		if (checkRemoteFile(mapsPath+"UM"+mapperID+".txt", reducerFull)) continue;
    	            // Execute the actual shuffle
    	            listPBs.add(new ProcessBuilder("scp", mapperFull+":"+splitsPath+"UM"+mapperID+".txt", reducerFull + ":" + mapsPath));
    	        }
    	        for (ProcessBuilder pb : listPBs) listPs.add(pb.start());
    	        for (Process p : listPs) p.waitFor();
        	}
	        System.out.println("[OK] SHUFFLE PHASE IS DONE - everything has been sent to machine "+reducerFull);
        }        
        pause();
        // ###########################################
        
        // ###########################################
        // [REDUCE PHASE]
        // On every reducer machine
        // run Slave in REDUCE mode
        // For each key assigned to that machine
        // ###########################################

        // Process Handlers for reducers
        List<ProcessBuilder> listRPBs = new ArrayList<>();
        List<Process> listRPs = new ArrayList<>();
        ProcessBuilder[] reducersProcessBuilder = new ProcessBuilder[nReducers];
        Process[] reducersProcess = new Process[nReducers];
        
        // Iterator on key per machine
        
        // [REDUCE PHASE 1]
        for (int k = 0; k < machineKeysMap.size(); k++) {
	        for (String m : machineKeysMap.keySet()) {
	        	if (k>=machineKeysMap.get(m).size()) continue;
	        	// For every machine, run a parallel process to reduce on the first key assigned to it
	        	String reducekey = machineKeysMap.get(m).get(k);
	        	String reducerID = machinesIDMap.get(m).toString();
	        	String reducerFull = String.format("%s%s%s", userPrefix, m, domain);
		        // Create string for the command
		        StringBuilder UMPath = new StringBuilder();
		        UMPath.append(slavePath).append(" 1 ").append(reducekey).append(" ").append(mapsPath).append("SM")
		        		.append(reducerID).append(".txt ");
		        for (String mapper : keysUMxMap.get(reducekey)) UMPath.append(mapsPath).append(mapper).append(".txt ");
		        // [LAUNCH REDUCE PHASE 1]
		        listRPBs.add(new ProcessBuilder("ssh", reducerFull, "java -jar "+UMPath.toString()));
	        }
	        for (ProcessBuilder pb : listRPBs) listRPs.add(pb.start());
	        for (Process p : listRPs) p.waitFor();
        }
        
        // [REDUCE PHASE 2]
        for (int k = 0; k < keys.length; k++) {
	        for (String m : machineKeysMap.keySet()) {
	        	if (k>=machineKeysMap.get(m).size()) continue;
	        	// For every machine, run a parallel process to reduce on the first key assigned to it
	        	String reducekey = machineKeysMap.get(m).get(k);
	        	String reducerID = machinesIDMap.get(m).toString();
	        	String reducerFull = String.format("%s%s%s", userPrefix, m, domain);
		        // Create string for the command
		        StringBuilder rmFilePath = new StringBuilder();
		        StringBuilder reduce2 = new StringBuilder();
		        rmFilePath.append(reducesPath).append("RM").append(reducerID).append(".txt");
		        reduce2.append(slavePath).append(" 2 ").append(reducekey).append(" ").append(mapsPath).append("SM")
		        		.append(reducerID).append(".txt ").append(rmFilePath.toString());
		        // [LAUNCH REDUCE PHASE 2]
		        listRPBs.add(new ProcessBuilder("ssh", reducerFull, "java -jar "+reduce2.toString()));
	        }
	        for (ProcessBuilder pb : listRPBs) listRPs.add(pb.start());
	        for (Process p : listRPs) p.waitFor();
        }
        
        
        /*for (int i = 0; i < nReducers; i++) {
        	// Get key & reducer info
        	String reducekey = keys[i];
        	int reducerID = i%nReducers;
        	String reducerName = machines[i%nReducers];
            String reducerFull = String.format("%s%s%s", userPrefix, reducerName, domain);
	        // Create string for the command
	        StringBuilder UMPath = new StringBuilder();
	        UMPath.append(slavePath).append(" 1 ").append(reducekey).append(" ").append(mapsPath).append("SM")
	        		.append(reducerID).append(".txt ");
	        for (String mapper : keysUMxMap.get(reducekey)) UMPath.append(mapsPath).append(mapper).append(".txt ");
	        // [LAUNCH REDUCE PHASE 1]
	        reducersProcessBuilder[i] = new ProcessBuilder("ssh", reducerFull, "java -jar "+UMPath.toString());
	        reducersProcess[i] = reducersProcessBuilder[i].start();
        }
        for (int i = 0; i < nReducers; i++) reducersProcess[i].waitFor();
	        
        for (int i = 0; i < nReducers; i++) {
        	// Get key & reducer info
        	String reducekey = keys[i];
        	int reducerID = i%nReducers;
        	String reducerName = machines[i%nReducers];
            String reducerFull = String.format("%s%s%s", userPrefix, reducerName, domain);
	        // Build the command
	        StringBuilder rmFilePath = new StringBuilder();
	        StringBuilder reduce2 = new StringBuilder();
	        rmFilePath.append(reducesPath).append("RM").append(reducerID).append(".txt");
	        reduce2.append(slavePath).append(" 2 ").append(reducekey).append(" ").append(mapsPath).append("SM")
	        		.append(reducerID).append(".txt ").append(rmFilePath.toString());
	        // [LAUNCH REDUCE PHASE 1]
	        reducersProcessBuilder[i] = new ProcessBuilder("ssh", reducerFull, "java -jar "+reduce2.toString());
	        reducersProcess[i] = reducersProcessBuilder[i].start();
        }
        for (int i = 0; i < nReducers; i++) reducersProcess[i].waitFor();*/
	        
        for (int i = 0; i < nReducers; i++) {
        	// Get key & reducer info
        	int reducerID = i%nReducers;
        	String reducerName = machines[i%nReducers];
            String reducerFull = String.format("%s%s%s", userPrefix, reducerName, domain);
	        StringBuilder rmFilePath = new StringBuilder();
	        rmFilePath.append(reducesPath).append("RM").append(reducerID).append(".txt");
            
	        // [EXTRACT RESULT FROM REDUCERS]
	        reducersProcessBuilder[i] = new ProcessBuilder("scp", reducerFull+":"+rmFilePath.toString(), reducesPath.substring(0, reducesPath.length()-1));
	        reducersProcess[i] = reducersProcessBuilder[i].start();
        }
        for (int i = 0; i < nReducers; i++) reducersProcess[i].waitFor();
        
        for (int i = 0; i < nReducers; i++) {
        	// Get key & reducer info
        	int reducerID = i%nReducers;
        	String reducerName = machines[i%nReducers];
            String reducerFull = String.format("%s%s%s", userPrefix, reducerName, domain);
	        StringBuilder rmFilePath = new StringBuilder();
	        rmFilePath.append(reducesPath).append("RM").append(reducerID).append(".txt");
	        
	        // Check if the file was received
	        if (!new File(rmFilePath.toString()).isFile()) {
	        	System.out.println("There was an error in copying file "+rmFilePath.toString()+" from machine "+reducerFull);
	        	return;
	        }
	    	
	    	// [OPEN FILE, READ RESULTS INTO MAP]
	        BufferedReader reader = new BufferedReader(new FileReader(rmFilePath.toString()));
	        while ((l = reader.readLine()) != null) {
	        	// System.out.println("Read from rmFile: "+l);
	        	String[] keyValue = l.split(" ");
	        	finalMap.put(keyValue[0], Integer.parseInt(keyValue[1]));
	        }
	        reader.close();	        
        }
        
        // [PRINT FINALMAP WITH RESULTS]
        System.out.println("[############################]");
        System.out.println(finalMap.toString());
        System.out.println("[############################]");
        
    }
    
    public boolean checkRemoteFile(String _path, String _machine) {
    	ProcessBuilder pb;
    	Process p;
    	BufferedReader input;
    	boolean toReturn = false;
    	try {
        	pb = new ProcessBuilder("ssh", _machine, "test -f \""+_path+"\" && echo found || echo not found ");
        	pb.redirectErrorStream(true);
			p = pb.start();
			input = new BufferedReader(new InputStreamReader(p.getInputStream()));
			if (input.readLine().equals("found")) toReturn = true;
			input.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return toReturn;
    }
}