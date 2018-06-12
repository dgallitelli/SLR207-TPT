import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;

public class InitFiles {
	
	PrintWriter writer;
	Map<Integer, String> files = new HashMap<>();
	static String rootPath = "/tmp/dgallitelli/";
	static String slavePath = "/cal/homes/dgallitelli/SLR207-TPT/SLAVE/out/artifacts/Slave/Slave.jar";
	
	public InitFiles() throws Exception {
		files.put(0, "Deer Beer River");
		files.put(1, "Car Car River");
		files.put(2, "Deer Car Beer");
		
		// Create splits dir if not existing
		File rootFolder = new File(rootPath+"splits"); 
		if (!rootFolder.exists())
			rootFolder.mkdir();
		
		// Create the splits files
		for (int i : files.keySet()) {
			writer = new PrintWriter(rootPath+"splits/S"+i+".txt", "UTF-8");
			writer.write(files.get(i));
			writer.close();
		}
		
		// Copy SLAVE/out/artifacts/Slave.jar into /tmp/dgallitelli
		File slavejar = new File(slavePath);
		Files.copy(slavejar.toPath(), new File(slavePath+"Slave.jar").toPath(), StandardCopyOption.REPLACE_EXISTING);
	}

	public Map<Integer, String> getFiles() {
		return files;
	}

	public void setFiles(Map<Integer, String> files) {
		this.files = files;
	}

}
