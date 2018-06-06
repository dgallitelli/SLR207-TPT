import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
// import java.util.concurrent.TimeUnit;

public class Master {

    private static String userPrefix = "dgallitelli@";
    private static String domain = ".enst.fr";
    private static String targetPath = "/tmp/dgallitelli/splits/";
    private static String slavePath = "/tmp/dgallitelli/slave.jar";

    private Master(){}

    public static void main(String[] args) throws IOException {
        Master ms = new Master();
        // ms.oldFoo();
        ms.newFoo();
    }

    private void newFoo() throws IOException {

        // Goal : Make Master copy three files from /tmp/dgallitelli/splits/ to 3 machines
        ProcessBuilder pb;
        Process p;
        BufferedReader input;
        String l, m;
        int i;

        // Define the target machines
        Map<String, Integer> targetMachines = new HashMap<>();
        targetMachines.put("c128-24", 0);
        targetMachines.put("c128-22", 1);
        targetMachines.put("c128-23", 2);

        // Define the MR map
        Map<String, List<String>> results = new HashMap<>();

        // Create the process for the creation of the folder, then copy
        for (String machine : targetMachines.keySet()) {
            m = String.format("%s%s%s", userPrefix, machine, domain);
            i = targetMachines.get(machine);

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
            pb = new ProcessBuilder("ssh", m, "mkdir -p " + targetPath);
            pb.redirectErrorStream(true);
            pb.start();
            System.out.println("[OK] Created dir on machine " + m);

            // ProcessBuilder for copying slavefile to remote
            pb = new ProcessBuilder("scp", slavePath, m + ":" + slavePath);
            pb.redirectErrorStream(true);
            pb.start();
            System.out.println("[OK] Copied slaveFile on machine " + m);

            // ProcessBuilder for copying splitfile to remote
            pb = new ProcessBuilder("scp", targetPath+"S0"+i+".txt",
                    m + ":" + targetPath);
            pb.redirectErrorStream(true);
            pb.start();
            System.out.println("[OK] Copied targetFiles on machine " + m);

            // ProcessBuilder to run slave program from /tmp/dgallitelli/
            pb = new ProcessBuilder("java", "-jar", slavePath);
            p = pb.start();
            System.out.println("[OK] Launched slave.jar on machine " + m);
            // Receive the output from the Slave currently running, update the map
            input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while ((l = input.readLine()) != null) {
                if (results.containsKey(l)) {
                    results.get(l).add("UM" + targetMachines.get(machine));
                } else {
                    List<String> newList = new ArrayList<>();
                    newList.add("UM" + targetMachines.get(machine));
                    results.put(l, newList);
                }
            }
        }

        // Print the mapping of files and machines
        /*for (String m2 : targetMachines.keySet()) System.out.println("UM"
                + targetMachines.get(m2) + " - " + m2);*/

        // Print the results
        for (String r : results.keySet())
            System.out.println(r + " - <" + results.get(r).toString() + ">");
        
    }
}