import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Vector;
import java.io.*;

/* Code belongs to Kartik Kohli & Jigar Thakkar :P */

public class DBSystem {

        final int CacheSize = 100;
        HashMap<String, Integer> tableNameToInt = new HashMap<>();
        Vector<Vector<Page>> listOfMaps = new Vector<Vector<Page>>();
        List<Page> pages = new ArrayList<Page>();
        int PAGESIZE,NUM_PAGES;
        String PATH_FOR_DATA;
        int numberOfTables;
        Queue<Page> cache = new LinkedList<Page>();


        public void readConfig(String configFilePath) {
                BufferedReader br = null;
                String str = "";
                System.out.println(configFilePath);
                //String[] params = new String[1000];
                int ctr = 0;
                try {
                        String currentLine;
                        br = new BufferedReader(new FileReader(configFilePath));
                        while((currentLine = br.readLine()) != null) {
                                str += currentLine + "\n";
                        }
                } catch (FileNotFoundException e) {
                        e.printStackTrace();
                } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                }
                String[] params = str.split("\n");
                System.out.println(params[0]);
                //Extracting Parameters from config file    
                PAGESIZE = Integer.valueOf(params[0].split(" ")[1]); 
                NUM_PAGES = Integer.valueOf(params[1].split(" ")[1]);
                PATH_FOR_DATA = params[2].split(" ")[1];
                System.out.println(PAGESIZE + " " + NUM_PAGES + " " + PATH_FOR_DATA);
                int i=3,tableCount=0;
                System.out.println(params.length+"");
                while(i<params.length) {
                        //if i > parax
                        tableNameToInt.put(params[++i], tableCount++);
                        Vector<Page> vectorPage = new Vector<Page>();
                        vectorPage.add(new Page(0, 0, 0, 0));
                        listOfMaps.add(vectorPage);
                        while(params[++i].equals("END")==false) {
                                //REad TABle attributes
                                System.out.println(params[i]);
                        }
                        i++;
                        System.out.println(i+"");
                }	
        }

        public void populateDBInfo() {
                BufferedReader br = null;
                System.out.println(tableNameToInt.size() + "");
                try {
                        String currentLine;
                        for(Map.Entry<String, Integer> entry: tableNameToInt.entrySet()) {
                                br = new BufferedReader(new FileReader(PATH_FOR_DATA + entry.getKey() + ".csv"));
                                System.out.println(entry.getKey());
                                Vector<Page> table = listOfMaps.get(entry.getValue());
                                int start=0,end=0,currentSize=0,pageNumber=0;
                                Page currentPage = table.get(pageNumber);
                                while((currentLine = br.readLine())!=null) {
                                		currentPage.currentSize += currentLine.length();
                                        if(currentPage.currentSize>PAGESIZE) {
                                        	    if(pageNumber==0) table.get(pageNumber).limit.end = end;
                                                table.add(new Page(pageNumber++,entry.getValue(),start,end));
                                                currentPage = table.get(pageNumber);
                                                start=end+1;
                                        }
                                        else {
                                                table.get(pageNumber).limit.end = end;
                                                end++;
                                        }
                                }
                        }

                } catch (FileNotFoundException e) {
                        //e.printStackTrace();
                        System.out.println("File Not Found:"+e.getLocalizedMessage()+"\n");
                } catch (IOException e) {
                        e.printStackTrace();
                }
        }

        public String getRecord(String tableName, int recordId) {
                int tableNumber = tableNameToInt.get(tableName);
                int first  = 0;
                int last   = listOfMaps.get(tableNumber).size() - 1;
                int  middle = (first + last)/2;
                while( first <= last )
                {
                        Page midPage = listOfMaps.get(tableNumber).get(middle);
                        System.out.println(midPage.limit.start + "End: " + midPage.limit.end);
                        if ( midPage.limit.end < recordId )
                                first = middle + 1;    
                        else if ( midPage.limit.start <= recordId && midPage.limit.end >= recordId) 
                        {
                                System.out.println("here: \n" + recordId + "\n");
                                //System.out.println(search + " found at location " + (middle + 1) + ".");
                                if (cache.contains(midPage))
                                {
                                        //if (cache.get(cache.size() - 1) != midPage)
                                        //{
                                        //Cache already has the Page.
                                        cache.remove(midPage);
                                        cache.add(midPage);
                                        //}
                                        System.out.println("HIT");
                                }
                                else
                                {
                                        if (cache.size() < NUM_PAGES)
                                        {
                                                //Cache not full. Adding a Page to cache;
                                                cache.add(midPage);
                                                System.out.println("New Page added to the LRU cache!\nPage Number: "+ midPage.pageNumber);
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
        
        public void insertRecord(String tableName, String record) {
        	int tableNumber = tableNameToInt.get(tableName);
        	int tablePages = listOfMaps.get(tableNumber).size();
        	Page lastPage = listOfMaps.get(tableNumber).get(tablePages - 1);
                System.out.println(lastPage.limit.end + "");
        	lastPage.currentSize += record.length(); 
        	if (lastPage.currentSize <= PAGESIZE)
        	{
        		lastPage.limit.end ++;
        	}
        	else
        	{
        		listOfMaps.get(tableNumber).add(new Page(lastPage.pageNumber + 1, lastPage.tableNumber, 0, 0));
        		Page addedPage = listOfMaps.get(tableNumber).get(tablePages);
        		addedPage.limit.end ++;
        	}
                System.out.println(lastPage.limit.end + "");
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
                int currentSize,tableNumber;
                
                public Page(int id,int tableNumber,int start,int end) {
                        this.pageNumber = id;
                        this.tableNumber = tableNumber;
                        this.limit = new Pair(start, end);
                }
        }

        public static void main(String[] args) 
        {
                DBSystem d = new DBSystem();
                d.readConfig("./config.txt");
                d.populateDBInfo();
                d.getRecord("employee", 1);
                d.getRecord("employee", 0);
                d.insertRecord("employee", "\"1\",\"abc\"");
                //System.out.println(d.PageSize);
        }
}
