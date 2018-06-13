import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;

public class Deploy {
	
    private static String userPrefix = "dgallitelli@";
    private static String domain = ".enst.fr";
    private static String rootPath = "/tmp/dgallitelli/";
    private static String splitsPath = "/tmp/dgallitelli/splits/";
    private static String mapsPath = "/tmp/dgallitelli/maps/";
    private static String reducesPath = "/tmp/dgallitelli/reduces/";
    private static String slavePath = "/tmp/dgallitelli/Slave.jar";
	
	public Deploy() {}
	
	public Deploy(String thisPC, Map<String, Integer> mapMachinesID) throws Exception {
		
		ProcessBuilder pb;
		Process p;
		BufferedReader input;
        String l, m;
        int i;
		
		for (String machine : mapMachinesID.keySet()) {
            m = String.format("%s%s%s", userPrefix, machine, domain);
            i = mapMachinesID.get(machine);

            // Start connection process
            System.out.println("[BEGIN] Starting connection to machine " + m);

            // ProcessBuilder for checking connection with hostname
            pb = new ProcessBuilder("ssh", m, "hostname");
            pb.redirectErrorStream(true);
            p = pb.start();
            p.waitFor();
            // Get output
            input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            l = input.readLine();
            if (!l.equals(machine)) {
                System.out.println("[ERR] Can't connect to machine " + m + " where l = "+l);
                continue;
            }
            System.out.println("[OK] Connection available with machine " + m);
            
			if (!machine.equals(thisPC)) {
                pb = new ProcessBuilder("ssh", m, "rm -rf /tmp/dgallitelli/");
                p = pb.start();
                p.waitFor();
                

                // ProcessBuilder for mkdir
                pb = new ProcessBuilder("ssh", m, "mkdir -p " +rootPath);
                p = pb.start();
                p.waitFor();
                System.out.println("[OK] Created rootDir on machine " + m);
                

                // ProcessBuilder for mkdir
                pb = new ProcessBuilder("ssh", m, "mkdir -p "+splitsPath+" "+mapsPath+" "+reducesPath);
                p = pb.start();
                p.waitFor();
                System.out.println("[OK] Created subdirs on machine " + m);

                // ProcessBuilder for copying slavefile to remote
                pb = new ProcessBuilder("scp", slavePath, m + ":" + slavePath);
                p = pb.start();
                System.out.println("[OK] Copied slaveFile on machine " + m);

                // ProcessBuilder for copying splitfile to remote
                pb = new ProcessBuilder("scp", splitsPath+"S"+i+".txt",
                        m + ":" + splitsPath);
                p = pb.start();
                System.out.println("[OK] Copied targetFiles on machine " + m);
            }
		}
		
	}

}
