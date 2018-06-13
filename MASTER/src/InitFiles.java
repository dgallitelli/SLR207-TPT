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
		files.put(3, "Beer Car River");
		
		createFolders();
		
		// Create the splits files
		for (int i : files.keySet()) {
			writer = new PrintWriter(rootPath+"splits/S"+i+".txt", "UTF-8");
			writer.write(files.get(i)+"\n");
			writer.close();
		}
	}
	
	public InitFiles(String _folder) throws Exception {
		
		createFolders();
		
		File folder = new File(_folder);
		// In case folder is provided, copy files and rename them
	    File[] _files = folder.listFiles();
	    if(_files!=null) {
	    	int i = 0;
	        for(File f: _files) {
	        	Files.copy(f.toPath(), new File(rootPath+"splits/S"+i+".txt").toPath(), StandardCopyOption.REPLACE_EXISTING);
	        	files.put(i, "");
	        	i++;
	        }
	    }
	}
	
	private void createFolders() throws Exception {
		
		// Create root dir if not existing
		File rootFolder = new File(rootPath); 
		if (!rootFolder.exists())
			rootFolder.mkdir();
		
		// Create splits dir if not existing
		File splitsFolder = new File(rootPath+"splits"); 
		if (!splitsFolder.exists())
			splitsFolder.mkdir();

		
		// Create splits dir if not existing
		File mapsFolder = new File(rootPath+"maps"); 
		if (!mapsFolder.exists())
			mapsFolder.mkdir();
		
		// Create splits dir if not existing
		File reducesFolder = new File(rootPath+"reduces"); 
		if (!reducesFolder.exists())
			reducesFolder.mkdir();
		
		// Copy SLAVE/out/artifacts/Slave.jar into /tmp/dgallitelli
		Files.copy(new File(slavePath).toPath(), new File(rootPath+"Slave.jar").toPath(), StandardCopyOption.REPLACE_EXISTING);
	}

	public Map<Integer, String> getFiles() {
		return files;
	}

	public void setFiles(Map<Integer, String> files) {
		this.files = files;
	}
	
	public void deleteFolder(File folder) {
	    File[] files = folder.listFiles();
	    if(files!=null) {
	        for(File f: files) {
	            if(f.isDirectory()) {
	                deleteFolder(f);
	            } else {
	                f.delete();
	            }
	        }
	    }
	}

}
