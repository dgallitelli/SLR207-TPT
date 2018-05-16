package com.SLR207;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder;
import java.util.ArrayList;
import java.util.List;

/**
 * @author dgallitelli
 *
 */

public class Deploy {
	
	public static void main(String[] args) {
		BufferedReader br = null, input = null;
		List<String> machines = null;
		ProcessBuilder pb = null;
		Process p = null;
		String l, slavePath = "/tmp/castelluccio/", userPrefix = "dgallitelli@", slaveFile = "slave.jar", domain = ".enst.fr";

		try {
			br = new BufferedReader(new FileReader("src/machines.txt"));
			machines = new ArrayList<>();
			String sCurrentLine;

			while ((sCurrentLine = br.readLine()) != null) machines.add(userPrefix+sCurrentLine);
			
			br.close();
			
			for (String m : machines) {
				// ProcessBuilder for checking connection with hostname
				pb = new ProcessBuilder("ssh", m, "hostname");
				pb.redirectErrorStream(true);
				// Process
				p = pb.start();
				// Get output
				input = new BufferedReader(new InputStreamReader(p.getInputStream()));
				l = userPrefix+input.readLine()+domain;
				if (!l.equals(m)) {
					System.out.println("[ERR] Can't connect to machine "+m);
					continue;
				}
				System.out.println("[OK] Connection available with machine "+m);
				
				// ProcessBuilder for mkdir
				pb = new ProcessBuilder("ssh", m, "mkdir -p "+slavePath);
				pb.redirectErrorStream(true);
				// Process
				p = pb.start();
				System.out.println("[OK] Created dir on machine "+m);
				
				// ProcessBuilder for copying slave to remote
				pb = new ProcessBuilder("scp", slavePath+slaveFile, m+":"+slavePath);
				pb.redirectErrorStream(true);
				// Process
				p = pb.start();
				System.out.println("[OK] Copied slaveFile on machine "+m);
				
				// ProcessBuilder for executing java
				pb = new ProcessBuilder("ssh", m, "java -jar "+slavePath+slaveFile);
				pb.redirectErrorStream(true);
				// Process
				p = pb.start();
				// Get output
				input = new BufferedReader(new InputStreamReader(p.getInputStream()));
				while ((l = input.readLine()) != null) {
					System.out.println(l);
				}
			}		

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// Clean the file openers
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

}

