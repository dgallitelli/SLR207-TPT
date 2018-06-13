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

    public static void main(String[] args) throws IOException {
    	try {
			InitFiles ifs = new InitFiles();
	        Master ms = new Master();
	        ms.newFoo(ifs.getFiles().keySet().size());
	        return;
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
    }
    
    @SuppressWarnings({ "resource" })
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
        Map<String, Integer> mapMachinesID = new HashMap<>();
        for (int j = 0; j < nMachines; j++)
        	mapMachinesID.put(classroom+"-"+(firstPC+j), j);
        
        // Dictionary UMx-Machines
        Map<String, String> mapUMxMachines = new HashMap<>();
        for (int j = 0; j < nMachines; j++)
        	mapUMxMachines.put("UM"+j, new String(classroom+"-"+(firstPC+j)));

        // Define the MR map
        Map<String, List<String>> mapKeysUMx = new HashMap<>();
        
        // Deploy Slave and files
        new Deploy(this.thisPC, mapMachinesID);
        
        // Get machines
    	String[] machines = mapMachinesID.keySet().toArray(new String[mapMachinesID.keySet().size()]);
    	
    	// ###########################################
        // [RUN MULTIPLE SLAVES IN PARALLEL, MAP MODE]
    	// ###########################################
    	
        ProcessBuilder[] mappersProcessBuilder = new ProcessBuilder[machines.length];
        Process[] mappersProcess = new Process[machines.length];
        for (int j = 0; j < machines.length; j++) {
			mappersProcessBuilder[j] = new ProcessBuilder("ssh", machines[j], "java -jar "+slavePath+" 0 "+splitsPath+"S"+mapMachinesID.get(machines[j])+".txt");
        	mappersProcess[j] = mappersProcessBuilder[j].start();
        	System.out.println("[OK] Launched slave.jar MAP MODE on machine " + machines[j]);
        }
        // Wait for their completion and read the outputs
        for (int j = 0; j < machines.length; j++) mappersProcess[j].waitFor();
        // Receive the outputs of the mappers
        for (int j = 0; j < machines.length; j++) {
            input = new BufferedReader(new InputStreamReader(mappersProcess[j].getInputStream()));
            while ((l = input.readLine()) != null) {
                if (mapKeysUMx.containsKey(l)) {
                    mapKeysUMx.get(l).add("UM" + mapMachinesID.get(machines[j]));
                } else {
                    List<String> newList = new ArrayList<>();
                    newList.add("UM" + mapMachinesID.get(machines[j]));
                    mapKeysUMx.put(l, newList);
                }
            }
        }
        
        // ###########################################

        // Print the results
        for (String r : mapKeysUMx.keySet())
        	System.out.println(r + " - <" + mapKeysUMx.get(r).toString() + ">");

        
    	String[] keys = mapKeysUMx.keySet().toArray(new String[mapKeysUMx.keySet().size()]);
    	int nReducers = nMachines < keys.length ? nMachines : keys.length;

        
        // ###########################################
        // [SHUFFLE PHASE]
        // For every key, find an available machine
        // To be used as a reducer
        // ###########################################
        
        // FinalMap for results
        Map<String, String> keysMachinesMap = new HashMap<>();
    	
        for (int i = 0; i < keys.length; i++) {
        	String reducekey = keys[i];
        	String reducerID = machines[i%nReducers];
            String reducerFull = String.format("%s%s%s", userPrefix, reducerID, domain);
            keysMachinesMap.put(reducekey, reducerID);
        	System.out.println("[KEY-PC] Key-PC pair: "+reducekey+" - "+keysMachinesMap.get(reducekey));        	
        
	        // [SHUFFLE PHASE]
	        List<String> mappers = mapKeysUMx.get(reducekey);
            ProcessBuilder[] shuffleProcessBuilder = new ProcessBuilder[mappers.size()];
            Process[] shuffleProcess = new Process[mappers.size()];
            
	        for (int j = 0; j < mappers.size(); j++) {
	        	String mapperFile = mappers.get(j);
	        	String mapper = mapUMxMachines.get(mapperFile);
	            int mapperID = Integer.parseInt(mapperFile.split("M")[1]);
	            String mapperFull = String.format("%s%s%s", userPrefix, mapper, domain);
	            
	            shuffleProcessBuilder[j] = new ProcessBuilder("scp", mapperFull+":"+splitsPath+"UM"+mapperID+".txt", reducerFull + ":" + mapsPath);
	            shuffleProcess[j] = shuffleProcessBuilder[j].start();
	        }
	        for (int j = 0; j < mappers.size(); j++) shuffleProcess[j].waitFor();
	        System.out.println("[OK] SHUFFLE PHASE IS DONE - everything has been sent to machine "+reducerFull);
        }
        // ###########################################
        
        // ###########################################
        // [REDUCE PHASE]
        // On every reducer machine
        // run Slave in REDUCE mode
        // ###########################################
        
        // FinalMap for results
        Map<String, Integer> finalMap = new HashMap<>();
        // Process Handlers for reducers
        ProcessBuilder[] reducersProcessBuilder = new ProcessBuilder[nReducers];
        Process[] reducersProcess = new Process[nReducers];
        
        for (int i = 0; i < nReducers; i++) {
        	// Get key & reducer info
        	String reducekey = keys[i];
        	int reducerID = i%nReducers;
        	String reducerName = machines[i%nReducers];
            String reducerFull = String.format("%s%s%s", userPrefix, reducerName, domain);
	        // Create string for the command
	        StringBuilder UMPath = new StringBuilder();
	        UMPath.append(slavePath).append(" 1 ").append(reducekey).append(" ").append(mapsPath).append("SM").append(reducerID).append(".txt ");
	        for (String mapper : mapKeysUMx.get(reducekey)) UMPath.append(mapsPath).append(mapper).append(".txt ");
	        // System.out.println("UMPATH = "+UMPath.toString());
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
	        reduce2.append(slavePath).append(" 2 ").append(reducekey).append(" ").append(mapsPath).append("SM").append(reducerID).append(".txt ").append(rmFilePath.toString());
	        // System.out.println("REDUCE2 = "+reduce2.toString());
	        // [LAUNCH REDUCE PHASE 1]
	        reducersProcessBuilder[i] = new ProcessBuilder("ssh", reducerFull, "java -jar "+reduce2.toString());
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
}