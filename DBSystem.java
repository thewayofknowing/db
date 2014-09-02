import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/* Code belongs to Kartik Kohli & Jigar Thakkar :P */

public class DBSystem {
	
	final int CacheSize = 100;
	HashMap<String, Integer> tableNameToInt = new HashMap<>();
	List<List<Pair>> listOfMaps = new ArrayList<List<Pair>>();
	List<Page> pages = new ArrayList<Page>();
	int pageSize,numPages;
	int numberOfTables;
	Queue<Page> cache = new LinkedList<Page>();
	
	public void readConfig(String configFilePath) {
	}

	public void populatePageInfo() {
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
