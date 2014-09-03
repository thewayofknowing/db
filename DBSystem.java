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
                //System.out.println(configFilePath);
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
                //System.out.println(params[0]);
                //Extracting Parameters from config file    
                PAGESIZE = Integer.valueOf(params[0].split(" ")[1]); 
                NUM_PAGES = Integer.valueOf(params[1].split(" ")[1]);
                PATH_FOR_DATA = params[2].split(" ")[1];
                //System.out.println(PAGESIZE + " " + NUM_PAGES + " " + PATH_FOR_DATA);
                int i=3,tableCount=0;
                //System.out.println(params.length+"");
                while(i<params.length) {
                        //if i > parax
                        tableNameToInt.put(params[++i], tableCount++);
                        Vector<Page> vectorPage = new Vector<Page>();
                        vectorPage.add(new Page(0, 0, 0, 0, params[i]));
                        //System.out.println(table.toString());
                        listOfMaps.add(vectorPage);
                        while(params[++i].equals("END")==false) {
                                //REad TABle attributes
                                //System.out.println(params[i]);
                        }
                        i++;
                        //System.out.println(i+"");
                }	
        }

        public void populateDBInfo() {
                BufferedReader br = null;
                try {
                        String currentLine;
                        for(Map.Entry<String, Integer> entry: tableNameToInt.entrySet()) {
                                br = new BufferedReader(new FileReader(PATH_FOR_DATA + entry.getKey() + ".csv"));
                                //System.out.println(entry.getKey());
                                Vector<Page> table = listOfMaps.get(entry.getValue());
                                int start=0,end=0,currentSize=0,pageNumber=0;
                                Page currentPage = table.get(pageNumber);
                                while((currentLine = br.readLine())!=null) {
                                        //System.out.println("size:" + currentPage.currentSize);

                                        if(currentPage.currentSize + currentLine.length()>PAGESIZE) {
                                                //System.out.println(start + ":" + end);
                                                //if(pageNumber==0)
                                                //{
                                                //        table.get(pageNumber).limit.end = end;
                                                //}
                                                start=end; 
                                                table.add(new Page(++pageNumber,entry.getValue(),start, end, entry.getKey()));
                                                //System.out.println(table.toString());
                                                currentPage = table.get(pageNumber);
                                        }
                                        else {
                                                currentPage.currentSize += currentLine.length();
                                                table.get(pageNumber).limit.end = ++end;
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
                        //System.out.println(midPage.limit.start + "End: " + midPage.limit.end);
                        if ( midPage.limit.end < recordId )
                                first = middle + 1;    
                        else if ( midPage.limit.start <= recordId && midPage.limit.end >= recordId) 
                        {
                                //System.out.println("here: \n" + recordId + "\n");
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
                //System.out.println(lastPage.limit.end + "");
                lastPage.currentSize += record.length(); 
                if (lastPage.currentSize <= PAGESIZE)
                {
                        lastPage.page = record;
                        if (cache.contains(lastPage))
                        {
                                cache.remove(lastPage);
                                System.out.println("HIT in insert");
                        }
                        else
                        {
                                //cache.add(lastPage);
                                System.out.println("MISSED during insert");
                        }
                        cache.add(lastPage);
                        lastPage.limit.end ++;
                }
                else
                {
                        listOfMaps.get(tableNumber).add(new Page(lastPage.pageNumber + 1, lastPage.tableNumber, 0, 0, tableName));
                        Page addedPage = listOfMaps.get(tableNumber).get(tablePages);
                        addedPage.limit.end ++;
                        addedPage.page = record;
                        if (cache.size() < NUM_PAGES)
                        {
                                cache.add(addedPage);
                                System.out.println("Newly created Page added to the cache!!");
                        }
                        else
                        {
                                Page replacedPage = cache.remove();
                                cache.add(addedPage);
                                System.out.println("Replaced a page in cache for the newly created page. Replaced Page: " + replacedPage.pageNumber);
                        }
                }
                //System.out.println(lastPage.limit.end + "");
        }
        public void flushPages()
        {

                for (int i = 0;i < cache.size();i ++)
                {
                        //System.out.println("here");
                        Page poppedPage = cache.remove();
                        //System.out.println(poppedPage.page);
                        try {
                                String finalPath = PATH_FOR_DATA + poppedPage.tableName + ".csv";
                                //System.out.println(finalPath);
                                //PrintWriter output = new PrintWriter(new BufferedWriter(new FileWriter(finalPath, true)));
                                if (poppedPage.page.length()!=0) {
                                         PrintWriter output = new PrintWriter(new BufferedWriter(new FileWriter(finalPath, true)));
                                        output.println(poppedPage.page);
                                        output.close();
                                }
                                //output.write(poppedPage.page);
                                cache.add(poppedPage);
                        }
                        catch(IOException e){
                                System.out.println("Failed to FLush!!");
                        }
                }
        }

        public static class Pair {
                public int start,end;
                public Pair(int a,int b) {
                        this.start = a;
                        this.end = b;
                }
        }

        public static class Page {
                public String page;
                public Integer pageNumber;
                public Pair limit;
                public int currentSize,tableNumber;
                public String tableName;
                public Page(int id,int tableNumber,int start,int end, String tableName) {
                        this.pageNumber = id;
                        this.tableNumber = tableNumber;
                        this.limit = new Pair(start, end);
                        this.tableName = tableName;
                        this.currentSize = 0;
                        this.page = "";
                }
        }

        public static void main(String[] args) 
        {
                DBSystem d = new DBSystem();
                d.readConfig("./config.txt");
                d.populateDBInfo();
                d.getRecord("student", 1);
                d.getRecord("student", 0);
                d.insertRecord("employee", "\"1\",\"abc\"");
                d.insertRecord("student","\"3\",\"Name 3\",\"UG2kX\"");
                d.flushPages();
                //System.out.println(d.PageSize);
        }
}
