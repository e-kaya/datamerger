package mergers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class CSVMerger {
//TODO for each item in the list align the columns and merge the corresponding file with aggregated file
	
	private String pathToData;
	private ArrayList<FileListItem> listOfFiles;
	private static final int COLUMN_ALIGNMENT_OFFSET = 3;
	
	public CSVMerger(String pathToData) {
		super();
		this.pathToData = pathToData;
		this.listOfFiles = generateListOfAllFiles (this.pathToData);
		testGenerateColumnAlignment();
	}

	private void testGenerateColumnAlignment(){
		
		final File aggFile = new File("./agg_data.csv");
		final File inputFile = new File("./data/en/en_0_2014_2015.csv");
		int[] align = null;
		
		try {
			align = generateColumnAlignment(aggFile, inputFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		/*
		 * For each item in the list
		 * Align the columns 
		 * Generate new lines and append to the agg_data.csv
		 * */
		mergeDataFileToAggregateData(aggFile, inputFile, align);
		
	}
	
	private void mergeDataFileToAggregateData(File aggFile, File inputFile, int[] alignment){
		int numLines = getNumberLines(inputFile);
		String[] lines = new String[numLines];
		String str = "";
        for(int count = 1; count < numLines; count++) {
            str = readLine(count,inputFile);
            lines[count] = alignRow(str,alignment);
        }

        try {
			appendToFile(lines, aggFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private String alignRow(String str, int[] alignment) {
		String[] strArr = str.split(",");
		String[] newString = new String[strArr.length];
		for (int i = 0; i < newString.length; i++) {
			newString[i] = "";
		}
		for (int i = 0; i < strArr.length; i++) {// BUG HERE
			newString[alignment[i]] = strArr[i];
		}
		str = newString[0];
		for (int i = 1; i < newString.length; i++) {
			str = str + "," + newString[i];
		}
		
		return str;
	}

	private int[] generateColumnAlignment(File aggFile, File inputFile) throws IOException{
		
		BufferedReader brAggFile = new BufferedReader(new FileReader(aggFile));
		BufferedReader brInputFile = new BufferedReader(new FileReader(inputFile));
		
		String aggLine = brAggFile.readLine();
		String inputLine = brInputFile.readLine();
		
		String[] aggColumns = aggLine.split(",");
		String[] inputColumns = inputLine.split(",");
		int[] alignment = new int[inputColumns.length];
		int eci = aggColumns.length;// eci: Extra Column Index
		ArrayList<String> extraColumnNames = new ArrayList<String>();
		
		for (int j = 0; j < inputColumns.length; j++){
			alignment[j] = -1;
		}
		
		for (int i = 0; i < inputColumns.length; i++) {
			for (int a = COLUMN_ALIGNMENT_OFFSET; a < aggColumns.length; a++) {
				if (aggColumns[a].equals(inputColumns[i])) {
					alignment[i] = a;
				}
			}
			
			if (alignment[i] == -1) {
				//expand the aggregated table, add new column
				System.out.println("Oops!");
				alignment[i] = eci;
				extraColumnNames.add(inputColumns[i]);
				eci++;
			}
			
		}
		
		brAggFile.close();
		brInputFile.close();
		
		expandFileColumnWise(aggFile, extraColumnNames);
		
		return alignment;
	}
	
	
	
	
	private void expandFileColumnWise(File aggFile,
			ArrayList<String> extraColumnNames) throws FileNotFoundException {
		int numLines = getNumberLines(aggFile);
		String[] lines = new String[numLines];
        for(int count = 0; count < numLines; count++) {
            lines[count] = readLine(count,aggFile);
        }
        for (String str : extraColumnNames) {
        	lines[0] = lines[0].concat("," + str);
            for(int i = 1; i < lines.length; i++){
            	lines[i].concat(",");
            }
        }

        try {
			writeFile(lines, aggFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	private void writeFile(String[] lines, File aFile) throws FileNotFoundException, IOException {
		if (aFile == null) {
			throw new IllegalArgumentException("File should not be null.");
		}
		if (!aFile.exists()) {
			throw new FileNotFoundException ("File does not exist: " + aFile);
		}
		if (!aFile.isFile()) {
			throw new IllegalArgumentException("Should not be a directory: " + aFile);
		}
		if (!aFile.canWrite()) {
			throw new IllegalArgumentException("File cannot be written: " + aFile);
		}

		BufferedWriter output = new BufferedWriter(new FileWriter(aFile));
		try {
			for(int count = 0; count < lines.length; count++) {
				output.write(lines[count]);
				if(count != lines.length - 1) {// This makes sure that an extra new line is not inserted at the end of the file
					output.newLine();
				}
			}
		}
		finally {
			output.close();
		}
	}
	
	private void appendToFile(String[] lines, File aFile) throws FileNotFoundException, IOException {
		if (aFile == null) {
			throw new IllegalArgumentException("File should not be null.");
		}
		if (!aFile.exists()) {
			throw new FileNotFoundException ("File does not exist: " + aFile);
		}
		if (!aFile.isFile()) {
			throw new IllegalArgumentException("Should not be a directory: " + aFile);
		}
		if (!aFile.canWrite()) {
			throw new IllegalArgumentException("File cannot be written: " + aFile);
		}

		BufferedWriter output = new BufferedWriter(new FileWriter(aFile, true));
		try {
			for(int count = 0; count < lines.length; count++) {
				output.write(lines[count]);
				if(count != lines.length - 1) {// This makes sure that an extra new line is not inserted at the end of the file
					output.newLine();
				}
			}
		}
		finally {
			output.close();
		}
	}
	
	// http://stackoverflow.com/questions/12217087/add-a-specific-string-at-the-end-of-a-line-of-file-in-java
	private int getNumberLines(File aFile) {
	    int numLines = 0;
	    try {

	        BufferedReader input =  new BufferedReader(new FileReader(aFile));
	            try {
	                String line = null;

	                while (( line = input.readLine()) != null){ //ReadLine returns the contents of the next line, and returns null at the end of the file.
	                    numLines++;
	                }
	  }
	  finally {
	    input.close();
	  }
	}
	catch (IOException ex){
	  ex.printStackTrace();
	}
	    return numLines;
	}
	
	public String readLine(int lineNumber, File aFile) {
        String lineText = "";
        try {

            BufferedReader input =  new BufferedReader(new FileReader(aFile));
                try {
                     for(int count = 0; count < lineNumber; count++) {
                        input.readLine();  //ReadLine returns the contents of the next line, and returns null at the end of the file.
                     }
                     lineText = input.readLine();
      }
      finally {
        input.close();
      }
    }
    catch (IOException ex){
      ex.printStackTrace();
    }
        return lineText;
    }
	

	private ArrayList<FileListItem> generateListOfAllFiles (String path) {
		
		final File folder = new File(path);
		return listFilesForFolder(folder);
		
	}
	
	// Shamelessly borrowed from StackOverflow
	// http://stackoverflow.com/questions/1844688/read-all-files-in-a-folder
	// Credit goes to the user "rich"
	private ArrayList<FileListItem> listFilesForFolder(final File folder) {
	    
		ArrayList<FileListItem> list = new ArrayList<FileListItem>();  
		
		for (final File fileEntry : folder.listFiles()) {
	        if (fileEntry.isDirectory()) {
	            listFilesForFolder(fileEntry);
	        } else {
	            System.out.println(fileEntry.getName());
	        	list.add(new FileListItem(fileEntry.getAbsolutePath(), fileEntry.getName()));
	        }
	    }
	    
	    return list;
	}
	
}
