import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Master {

    private static String userPrefix = "dgallitelli@";
    private static String domain = ".enst.fr";
    private static String targetPath = "/tmp/dgallitelli/splits/";

    public static void main(String[] args) throws IOException {
        //oldFoo();

        // Goal : Make Master copy three files from /tmp/dgallitelli/splits/ to 3 machines
        ProcessBuilder pb;
        Process p;
        BufferedReader input;
        String l;

        // Define the target machines
        List<String> targetMachines;
        targetMachines = new ArrayList<>();
        targetMachines.add("dgallitelli@c129-21.enst.fr");
        targetMachines.add("dgallitelli@c129-22.enst.fr");
        targetMachines.add("dgallitelli@c129-23.enst.fr");

        // Create the process for the creation of the folder, then copy
        for (String m : targetMachines) {
            System.out.println("[BEGIN] Starting connection to machine " + m);
            // ProcessBuilder for checking connection with hostname
            pb = new ProcessBuilder("ssh", m, "hostname");
            pb.redirectErrorStream(true);
            // Process
            p = pb.start();
            // Get output
            input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            l = userPrefix + input.readLine() + domain;
            if (!l.equals(m)) {
                System.out.println("[ERR] Can't connect to machine " + m + " where l = "+l);
                continue;
            }
            System.out.println("[OK] Connection available with machine " + m);

            // ProcessBuilder for mkdir
            pb = new ProcessBuilder("ssh", m, "mkdir -p " + targetPath);
            pb.redirectErrorStream(true);
            // Process
            pb.start();
            System.out.println("[OK] Created dir on machine " + m);

            // ProcessBuilder for copying slave to remote
            pb = new ProcessBuilder("scp", targetPath+"*", m + ":" + targetPath+"*");
            pb.redirectErrorStream(true);
            // Process
            pb.start();
            System.out.println("[OK] Copied targetFiles on machine " + m);
        }

    }

    private static void oldFoo(){
        String output;
        long startTime, endTime, totalTime;

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
