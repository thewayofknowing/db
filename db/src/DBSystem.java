import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Vector;

/* Code belongs to Kartik Kohli & Jigar Thakkar :P */

public class DBSystem {
	
	final int CacheSize = 100;
	HashMap<String, Integer> tableNameToInt = new HashMap<>();
	Vector<Vector<Page>> listOfMaps = new Vector<Vector<Page>>();
	//List<Page> pages = new ArrayList<Page>();
	int PAGESIZE,NUM_PAGES;
	String PATH_FOR_DATA;
	int numberOfTables;
	Queue<Page> cache = new LinkedList<Page>();
	
	public void main(String[] args) {
		readConfig("config.txt");
	}
	
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
				Vector<Page> table = listOfMaps.get(entry.getValue());
				int start=0,end=0,currentSize=0,pageNumber=0;
				while((currentLine = br.readLine())!=null) {
					currentSize+=currentLine.length();
					if(currentSize>PAGESIZE) {
						table.add(new Page(pageNumber++,start,end));
						start=end+1;
					}
					end++;
				}
			}
				
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String getRecord(String tableName, int recordId) {
		int tableNumber = tableNameToInt.get(tableName);
	    int first  = 0;
	    int last   = listOfMaps.size();
	    int middle = (first + last)/2;
	    while( first <= last )
	    {
	      Page midPage = listOfMaps.get(tableNumber).get(middle);
	      if ( midPage.limit.end < recordId )
	        first = middle + 1;    
	      else if ( midPage.limit.start >= recordId && midPage.limit.end <= recordId) 
	      {
	        //System.out.println(search + " found at location " + (middle + 1) + ".");
	    	if (cache.contains(midPage))
	    	{
    			//Cache already has the Page.
    			cache.remove(midPage);
    			cache.add(midPage);	    		
	    		System.out.println("HIT");
	    	}
	    	else
	    	{
	    		if (cache.size() < NUM_PAGES)
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
		public Pair(int a,int b) {
			this.start = a;
			this.end = b;
		}
	}
	
	public static class Page {
		String page;
		Integer pageNumber;
		Pair limit;
		public Page(int id,int start,int end) {
			this.pageNumber = id;
			this.limit = new Pair(start, end);
		}
	}
}
