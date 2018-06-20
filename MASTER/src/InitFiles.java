import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class InitFiles {
	
	PrintWriter writer;
	Map<Integer, String> files = new HashMap<>();
	static String rootPath = "/tmp/dgallitelli/";
	static String slavePath = "/cal/homes/dgallitelli/SLR207-TPT/SLAVE/out/artifacts/Slave/Slave.jar";
	static long maxFileSize = 5*1024; //Max file size = 20KB
	
	public InitFiles() {}
	
	public InitFiles(String _inputPath) throws IOException {		
		
		createFolders();
		
		if (_inputPath.equals("")) {
			// No files provided - test
			files.put(0, "Deer Beer River Cat");
			files.put(1, "Car Car River");
			files.put(2, "Deer Car Beer Cat");
			files.put(3, "Beer Car River");
			
			// Create the splits files
			for (int i : files.keySet()) {
				writer = new PrintWriter(rootPath+"splits/S"+i+".txt", "UTF-8");
				writer.write(files.get(i)+"\n");
				writer.close();
			}			
		}
		
		// Check if the input is a folder or a file
		File inputPath = new File(_inputPath);
		if (inputPath.isDirectory()) {
			// Handle directory given as path
		    File[] _files = inputPath.listFiles();
		    if(_files!=null) {
		    	int i = 0;
		        for(File f: _files) {
		        	copyTmpAndUpdateMap(f, i);
		        	i++;
		        }
		    }
		} else {
			// Handle the file case
			long fileSize = inputPath.length();
			if (fileSize > 10*1024) {
				// TODO: case of big file, split it and run it
				generateSmallerFiles(inputPath);
			} else {
				// Single small file - map it on one PC
				copyTmpAndUpdateMap(inputPath, 0);
			}
		}
	}
	
	private void createFolders() throws IOException {
		File rootFolder = new File(rootPath);
		deleteFolder(rootFolder);
		
		// Create root dir if not existing
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

	public void copyTmpAndUpdateMap(File f, int i) throws IOException {
    	Files.copy(f.toPath(), new File(rootPath+"splits/S"+i+".txt").toPath(), StandardCopyOption.REPLACE_EXISTING);
    	files.put(i, "");		
	}
	
	public void generateSmallerFiles(File f) throws IOException {
		long nFiles = f.length()/maxFileSize;
		Scanner sc = new Scanner(f);
		for (int i = 0; i < nFiles; i++) {
			File sFile = new File(rootPath+"splits/S"+i+".txt");
			writer = new PrintWriter(sFile.toString(), "UTF-8");
			while (sc.hasNext()) {
				writer.write(sc.next());
				if (sFile.length() >= maxFileSize) break;
			}
			writer.close();
		}
		sc.close();
	}
}
