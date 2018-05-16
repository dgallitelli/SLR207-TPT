import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Master {

    private static String userPrefix = "dgallitelli@";
    private static String domain = ".enst.fr";
    private static String targetPath = "/tmp/dgallitelli/splits/";
    private static String slavePath = "/tmp/dgallitelli/slave.jar";

    public Master(){}

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
        targetMachines.put("c129-21", 0);
        targetMachines.put("c129-22", 1);
        targetMachines.put("c129-23", 2);

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

            // ProcessBuilder for copying splitfile to remote
            pb = new ProcessBuilder("scp", targetPath+"S"+i+".txt",
                    m + ":" + targetPath+"*"+"S"+i+".txt");
            pb.redirectErrorStream(true);
            pb.start();
            System.out.println("[OK] Copied targetFiles on machine " + m);

            // ProcessBuilder to run slave program from /tmp/dgallitelli/
            pb = new ProcessBuilder("java", "-jar", slavePath);
            pb.start();
            System.out.println("[OK] Launched slave.jar on machine " + m);
        }

        // Print the mapping of files and machines
        for (String m2 : targetMachines.keySet()) System.out.println("UM"
                + targetMachines.get(m2) + " - " + m2);
    }

    private void oldFoo(){
        long startTime, endTime;

        ProcessBuilder pb = new ProcessBuilder("java", "-jar", "/tmp/castelluccio/slave.jar");
        // Redirect Error to stream
        pb.redirectErrorStream(true);
        pb.inheritIO();

        try {
            startTime = System.currentTimeMillis();
            // Start the process
            Process p = pb.start();

            boolean b = p.waitFor(3, TimeUnit.SECONDS);
            if (!b) {
                // Timeout hit
                p.destroy();
                throw new InterruptedException();
            }

            // Normal termination

            // Get the word from stream
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            StringBuilder builder = new StringBuilder(); String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
                builder.append(System.getProperty("line.separator"));
            }
            String result = builder.toString();

            endTime = System.currentTimeMillis();

            // Print output
            System.out.println(result);
            System.out.println("MASTER: "+(endTime-startTime)+" ms");

        } catch (InterruptedException e) {
            System.out.println("Timeout hit. Try again.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
