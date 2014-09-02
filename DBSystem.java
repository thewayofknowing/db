import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Vector;

/* Code belongs to Kartik Kohli & Jigar Thakkar :P */

public class DBSystem {
	
	final int CacheSize = 100;
	HashMap<String, Integer> tableNameToInt = new HashMap<>();
	Vector<Vector<Pair>> listOfMaps = new Vector<Vector<Pair>>();
	List<Page> pages = new ArrayList<Page>();
	int pageSize,numPages;
	int numberOfTables;
	Queue<Page> cache = new LinkedList<Page>();
	
	public void readConfig(String configFilePath) {
		
	}

	public void populatePageInfo() {
	}

	public String getRecord(String tableName, int recordId) {
		int tableNumber = tableNameToInt.get(tableName);
	    first  = 0;
	    last   = listOfMaps.size();
	    middle = (first + last)/2;
	    while( first <= last )
	    {
	      Pair mid = listOfMaps[tableNumber][middle];
	      if ( mid. < recordId )
	        first = middle + 1;    
	      else if ( array[middle] == search ) 
	      {
	        System.out.println(search + " found at location " + (middle + 1) + ".");
	        break;
	      }
	      else
	         last = middle - 1;
	 
	      middle = (first + last)/2;
	   }
		listOfMaps[tableNumber][] 
		return "record";
	}
	
	public static class Pair {
		public int start,end;
	}
	
	public static class Page {
		String page;
	}
}
