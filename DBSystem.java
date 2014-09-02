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
	Vector<Vector<Page>> listOfMaps = new Vector<Vector<Page>>();
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
	      Page midPage = listOfMaps[tableNumber][middle];
	      if ( midPage.limit.end < recordId )
	        first = middle + 1;    
	      else if ( midPage.limit.start <= recordId && midPage.limit.end >= recordId) 
	      {
	        //System.out.println(search + " found at location " + (middle + 1) + ".");
	    	if (cache.contains(midPage))
	    	{
	    		if (cache[cache.size() - 1] != midPage])
	    		{
	    			//Cache already has the Page.
	    			cache.remove(midPage);
	    			cache.add(midPage);
	    		}
	    		System.out.println("HIT");
	    	}
	    	else
	    	{
	    		if (cache.size < numPages)
	    		{
	    			//Cache not full. Adding a Page to cache;
	    			cache.add(midPage);
	    		}
	    		else
	    		{
	    			//Cache full and some page is replaced.
	    			Page replacedPage = cache.remove();
	    			cache.add(midPage);
	    			System.out.println("MISS" + replacedPage.pageNumber);
	    		}
	    	}
	        break;
	      }
	      else
	         last = middle - 1;
	 
	      middle = (first + last)/2;
	   }
		return "record";
	}
	
	public static class Pair {
		public int start,end;
	}
	
	public static class Page {
		String page;
		Integer pageNumber;
		Pair limit;
	}
}
