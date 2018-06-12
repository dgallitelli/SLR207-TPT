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

	private static String classroom = "c125";
	private static int firstPC = 19;
    private static String userPrefix = "dgallitelli@";
    private static String domain = ".enst.fr";
    private static String splitsPath = "/tmp/dgallitelli/splits/";
    private static String mapsPath = "/tmp/dgallitelli/maps/";
    private static String reducesPath = "/tmp/dgallitelli/reduces/";
    private static String slavePath = "/tmp/dgallitelli/Slave.jar";

    private Master(){}

    public static void main(String[] args) throws IOException {
    	try {
			InitFiles ifs = new InitFiles();
	        Master ms = new Master();
	        ms.newFoo(ifs.getFiles().keySet().size());
	        return;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
    }

    private void newFoo(int nMachines) throws IOException {

        // Goal : Make Master copy three files from /tmp/dgallitelli/splits/ to 3 machines
        ProcessBuilder pb;
        Process p;
        BufferedReader input;
        String l, m;
        int i;

        // Define the target machines
        Map<String, Integer> mapMachinesID = new HashMap<>();
        for (int j = 0; j < nMachines; j++)
        	mapMachinesID.put(classroom+"-"+(firstPC+j), j);
        
        // Dictionary UMx-Machines
        Map<String, String> mapUMxMachines = new HashMap<>();
        for (int j = 0; j < nMachines; j++)
        	mapUMxMachines.put("UM"+j, new String(classroom+"-"+(firstPC+j)));
        

        // Define the MR map
        Map<String, List<String>> mapResults = new HashMap<>();

        // Create the process for the creation of the folder, then copy
        for (String machine : mapMachinesID.keySet()) {
            m = String.format("%s%s%s", userPrefix, machine, domain);
            i = mapMachinesID.get(machine);

            // Start connection process
            System.out.println("[BEGIN] Starting connection to machine " + m);

            // ProcessBuilder for checking connection with hostname
            pb = new ProcessBuilder("ssh", m, "hostname");
            pb.redirectErrorStream(true);
            p = pb.start();
            // Get output
            input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            l = input.readLine();
            if (!l.equals(machine)) {
                System.out.println("[ERR] Can't connect to machine " + m + " where l = "+l);
                continue;
            }
            System.out.println("[OK] Connection available with machine " + m);

            // ProcessBuilder for mkdir
            pb = new ProcessBuilder("ssh", m, "mkdir -p " + splitsPath+" "+mapsPath+" "+reducesPath);
            pb.redirectErrorStream(true);
            pb.start();
            System.out.println("[OK] Created dir on machine " + m);

            // ProcessBuilder for copying slavefile to remote
            pb = new ProcessBuilder("scp", slavePath, m + ":" + slavePath);
            pb.redirectErrorStream(true);
            pb.start();
            System.out.println("[OK] Copied slaveFile on machine " + m);

            // ProcessBuilder for copying splitfile to remote
            pb = new ProcessBuilder("scp", splitsPath+"S"+i+".txt",
                    m + ":" + splitsPath);
            pb.redirectErrorStream(true);
            pb.start();
            System.out.println("[OK] Copied targetFiles on machine " + m);

            // ProcessBuilder to run slave program in MAP MODE from /tmp/dgallitelli/
            pb = new ProcessBuilder("ssh", m, "java -jar "+slavePath+" 0 "+splitsPath+"S"+i+".txt");
            p = pb.start();
            System.out.println("[OK] Launched slave.jar MAP MODE on machine " + m);
            // Receive the output from the Slave currently running, update the map
            input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while ((l = input.readLine()) != null) {
                if (mapResults.containsKey(l)) {
                    mapResults.get(l).add("UM" + mapMachinesID.get(machine));
                } else {
                    List<String> newList = new ArrayList<>();
                    newList.add("UM" + mapMachinesID.get(machine));
                    mapResults.put(l, newList);
                }
            }
        }

        // Print the results
        for (String r : mapResults.keySet())
        	System.out.println(r + " - <" + mapResults.get(r).toString() + ">");
        
        // Setup the reducer Machine
        String reducer = (String) mapMachinesID.keySet().toArray()[0];
        String reducerFull = String.format("%s%s%s", userPrefix, reducer, domain);
        
        // Choose the key for reduce
        String reducekey = mapResults.entrySet().iterator().next().getKey();
        
        // [SHUFFLE PHASE]
        // Look for UMx files to send to the appropriate machine
        // targetMachine contains as value the x in UMx - mapResults contains the keys-UMx map
        List<String> mappers = mapResults.get(reducekey);
        for (String mapperFile : mappers) {
        	String mapper = mapUMxMachines.get(mapperFile);
            int mapperID = Integer.parseInt(mapperFile.split("M")[1]);
            String mapperFull = String.format("%s%s%s", userPrefix, mapper, domain);
        	// Every machine has to send their file UMx.txt, if any to the current machine
            pb = new ProcessBuilder("scp", mapperFull+":"+splitsPath+"UM"+mapperID+".txt", reducerFull + ":" + mapsPath);
            pb.start();
        }
        System.out.println("[OK] SHUFFLE PHASE IS DONE - everything has been sent to machine "+reducerFull);
        
        int reducerID = mapMachinesID.get(reducer);
        
        // [REDUCE PHASE]
        // New process on target machine to run Slave in REDUCE MODE
        
        // Create string for the command
        StringBuilder UMPath = new StringBuilder();
        UMPath.append(slavePath).append(" 1 ").append(reducekey).append(" ").append(mapsPath).append("SM").append(reducerID).append(".txt ");
        // Get the code of every mapper and build the argument
        for (String mapper : mappers)
        	UMPath.append(mapsPath).append(mapper).append(".txt ");
        System.out.println(UMPath.toString());
        // Build the process and run it
        pb = new ProcessBuilder("ssh", reducerFull, "java -jar "+UMPath.toString());
        p = pb.start();
        System.out.println("[OK] Launched Slave.jar REDUCE MODE on machine " + reducerFull);
        
        // [REDUCE PHASE 2]
        // New process on target machine to run Slave in REDUCE MODE 2
        
        // Build the command
        StringBuilder rmFilePath = new StringBuilder();
        StringBuilder reduce2 = new StringBuilder();
        rmFilePath.append(reducesPath).append("RM").append(reducerID).append(".txt");
        reduce2.append(slavePath).append(" 1 ").append(reducekey).append(" ").append(mapsPath).append("SM").append(reducerID).append(".txt ").append(rmFilePath.toString());
        // Build the process and run it
        pb = new ProcessBuilder("ssh", reducerFull, "java -jar "+reduce2.toString());
        p = pb.start();
        System.out.println("[OK] Launched Slave.jar REDUCE MODE 2 on machine " + reducerFull);
        
        // [EXTRACT RESULT FROM REDUCE MACHINE]
        pb = new ProcessBuilder("scp", reducerFull+":"+rmFilePath.toString(), reducesPath.substring(0, reducesPath.length()-1));
        pb.start();
        if (!new File(rmFilePath.toString()).isFile()) {
        	System.out.println("There was an error in copying file "+rmFilePath.toString()+" from machine "+reducerFull);
        	return;
        }
    	System.out.println("[OK] Obtained "+rmFilePath.toString()+" from machine "+reducerFull);
       
        
        // [OPEN FOR RESULTS]
        BufferedReader reader = new BufferedReader(new FileReader(rmFilePath.toString()));
        while ((l = reader.readLine()) != null)
        {
            System.out.println(l);
        }
        reader.close();
        
    }
}