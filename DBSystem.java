import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Vector;

/* Code belongs to Kartik Kohli & Jigar Thakkar :P */

public class DBSystem {
	
	final int CacheSize = 100;
	HashMap<String, Integer> tableNameToInt = new HashMap<>();
	Vector<Vector<Pair>> listOfMaps = new Vector<Vector<Pair>>();
	List<Page> pages = new ArrayList<Page>();
	int PAGESIZE,NUM_PAGES;
	String PATH_FOR_DATA;
	int numberOfTables;
	Vector<Integer> x = new Vector<>();
	Queue<Page> cache = new LinkedList<Page>();
	
	public void readConfig(String configFilePath) {
		BufferedReader br = null;
		String str = "";
		try {
			String currentLine;
			br = new BufferedReader(new FileReader(configFilePath));
			while((currentLine = br.readLine()) != null) {
				str += currentLine;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String[] params = str.split("\n");
		
		//Extracting Parameters from config file    
		PAGESIZE = Integer.valueOf(params[0].split(" ")[1]); 
		NUM_PAGES = Integer.valueOf(params[1].split(" ")[1]);
		PATH_FOR_DATA = params[2].split(" ")[1];
		int i=3,tableCount=0;
		while(i<params.length) {
			tableNameToInt.put(params[++i], tableCount++);
			while(params[++i].equals("END")==false) {
				//REad TABle attributes
			}
		}	
	}

	public void populatePageInfo() {
		BufferedReader br = null;
		try {
			String currentLine;
			for(Map.Entry<String, Integer> entry: tableNameToInt.entrySet()) {
				br = new BufferedReader(new FileReader(PATH_FOR_DATA + entry.getKey()));
				while((currentLine = br.readLine())!=null) {
					
				}
			}
				
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String getRecord(String tableName, int recordId) {
		return "record";
	}
	
	public static class Pair {
		int start,end;
	}
	
	public static class Page {
		String page;
	}
}
