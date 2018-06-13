import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
// import java.util.concurrent.TimeUnit;

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

        ProcessBuilder pb;
        Process p;
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

        // [RUBN MULTIPLE SLAVES IN PARALLEL, MAP MODE]
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

        // Print the results
        for (String r : mapKeysUMx.keySet())
        	System.out.println(r + " - <" + mapKeysUMx.get(r).toString() + ">");
        
        // Setup the reducer Machine
        String reducer = (String) mapMachinesID.keySet().toArray()[0];
        String reducerFull = String.format("%s%s%s", userPrefix, reducer, domain);
        
        // FinalMap for results
        Map<String, Integer> finalMap = new HashMap<>();
        
        // Choose the key for reduce
        for (String reducekey : mapKeysUMx.keySet()) {
        	
        	System.out.println("[KEY] Working with key: "+reducekey);
        
	        // [SHUFFLE PHASE]
	        // Look for UMx files to send to the appropriate machine
	        // targetMachine contains as value the x in UMx - mapResults contains the keys-UMx map
	        List<String> mappers = mapKeysUMx.get(reducekey);
	        for (String mapperFile : mappers) {
	        	String mapper = mapUMxMachines.get(mapperFile);
	            int mapperID = Integer.parseInt(mapperFile.split("M")[1]);
	            String mapperFull = String.format("%s%s%s", userPrefix, mapper, domain);
	        	// Every machine has to send their file UMx.txt, if any to the current machine
	            pb = new ProcessBuilder("scp", mapperFull+":"+splitsPath+"UM"+mapperID+".txt", reducerFull + ":" + mapsPath);
	            p = pb.start();
	            p.waitFor();
	        }
	        // System.out.println("[OK] SHUFFLE PHASE IS DONE - everything has been sent to machine "+reducerFull);
	        
	        int reducerID = mapMachinesID.get(reducer);
	        
	        // [REDUCE PHASE]
	        // New process on target machine to run Slave in REDUCE MODE
	        
	        // Create string for the command
	        StringBuilder UMPath = new StringBuilder();
	        UMPath.append(slavePath).append(" 1 ").append(reducekey).append(" ").append(mapsPath).append("SM").append(reducerID).append(".txt ");
	        // Get the code of every mapper and build the argument
	        for (String mapper : mappers)
	        	UMPath.append(mapsPath).append(mapper).append(".txt ");
	        //System.out.println(UMPath.toString());
	        // Build the process and run it
	        pb = new ProcessBuilder("ssh", reducerFull, "java -jar "+UMPath.toString());
            p = pb.start();
            p.waitFor();
	        // System.out.println("[OK] Launched Slave.jar REDUCE MODE on machine " + reducerFull);
	        
	        // [REDUCE PHASE 2]
	        // New process on target machine to run Slave in REDUCE MODE 2
	        
	        // Build the command
	        StringBuilder rmFilePath = new StringBuilder();
	        StringBuilder reduce2 = new StringBuilder();
	        rmFilePath.append(reducesPath).append("RM").append(reducerID).append(".txt");
	        reduce2.append(slavePath).append(" 1 ").append(reducekey).append(" ").append(mapsPath).append("SM").append(reducerID).append(".txt ").append(rmFilePath.toString());
	        // Build the process and run it - don't need output here
	        //System.out.println(reduce2.toString());
	        pb = new ProcessBuilder("ssh", reducerFull, "java -jar "+reduce2.toString());
	        p = pb.start();
            p.waitFor();
	        // System.out.println("[OK] Launched Slave.jar REDUCE MODE 2 on machine " + reducerFull);
	        
	        // [EXTRACT RESULT FROM REDUCE MACHINE]
	        pb = new ProcessBuilder("scp", reducerFull+":"+rmFilePath.toString(), reducesPath.substring(0, reducesPath.length()-1));
            p = pb.start();
            p.waitFor();
	        if (!new File(rmFilePath.toString()).isFile()) {
	        	System.out.println("There was an error in copying file "+rmFilePath.toString()+" from machine "+reducerFull);
	        	return;
	        }
	    	// System.out.println("[OK] Obtained "+rmFilePath.toString()+" from machine "+reducerFull);
	    	
	    	// [OPEN FILE, READ RESULTS INTO MAP]
	        BufferedReader reader = new BufferedReader(new FileReader(rmFilePath.toString()));
	        while ((l = reader.readLine()) != null) {
	        	// System.out.println("Read from rmFile: "+l);
	        	String[] keyValue = l.split(" ");
	        	finalMap.put(keyValue[0], Integer.parseInt(keyValue[1]));
	        }
	        reader.close();
	        
	        // [DELETE FILE, JUST TO AVOID PROBLEMS]
	        File rm = new File(rmFilePath.toString());
	        rm.delete();
	        
        }
        
        // [PRINT FINALMAP WITH RESULTS]
        System.out.println("[#################]");
        System.out.println(finalMap.toString());
        System.out.println("[#################]");
        
    }
}