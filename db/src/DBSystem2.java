
import org.gibello.zql.*;



public class DBSystem2 {
	
	public static void main(String[] args) throws ParseException  {
		     
			  ZqlParser p = null;
 	          System.out.println("Reading SQL from stdin (quit; or exit; to quit)");
	          //Scanner s = new Scanner(System.in);
	          //String input = s.nextLine();
	          //System.out.println("Scanned: " + input);
 	          p = new ZqlParser(System.in);  
 	         ZStatement st = p.readStatement();
 	         queryType((ZQuery)st);
 	         
	          //p.initParser(new ByteArrayInputStream(input.getBytes()));
		      //queryType(p);
	}

	public static void queryType(ZQuery st) throws ParseException {
		/*
		Determine the type of the query (select/create) and
		invoke appropriate method for it.
		*/
		String type = st.toString().split(" ")[0];
		if(type.equalsIgnoreCase("create")) {
			System.out.println("QueryType:" + type);
			createCommand(st);
		}
		else if(type.equalsIgnoreCase("select")) {
			System.out.println("QueryType:" + type);
			selectCommand(st);
		}
		else {
			System.out.println("Invalid Command");
		}
	}
	static void createCommand(ZQuery st) {
		/*
		Use any SQL parser to parse the input query. Check if the table doesn't exists
		and execute the query.
		The execution of the query creates two files : <tablename>.data and
		<tablename>.csv. An entry should be made in the system config file used
		in previous deliverable.
		Print the query tokens as specified at the end.
		**format for the file is given below
		*/
	}
	static void selectCommand(ZQuery st) throws ParseException {
		/*
		Use any SQL parser to parse the input query. Perform all validations (table
		name, attributes, datatypes, operations). Print the query tokens as specified
		below.
		*/
		

        //System.out.println(st.getWhere().toString()); // Display the statement
        System.out.println("TableName: " + st.getFrom());
        System.out.println("Columns: " + st.getSelect());
        
        //Where clause
        if(st.getWhere()!=null) {
        	System.out.println("Condition: " + st.getWhere());
        }
        else {
            System.out.println("Condition:N/A");
        }
        //Order by Clause
        if(st.getOrderBy()!=null) {
        	System.out.println("Orderby: " + st.getOrderBy());
        }
        else {
        	System.out.println("Orderby:N/A");
        }
        //Group by clause
        if(st.getGroupBy()!=null) {
        	System.out.println("Groupby: " + st.getGroupBy());
        	//Having clause
        	if(st.getGroupBy().toString().contains("having")) {
        		System.out.println("Having:" + st.getGroupBy().toString().split("having")[1]);
        	}
        }
        else {
        	System.out.println("Groupby:N/A");
        }
        
        /*
        if(st instanceof ZQuery) { // An SQL query: query the DB
          queryDB((ZQuery)st);
        } else if(st instanceof ZInsert){ // An SQL insert
          insertDB((ZInsert)st);
        }
        */
      
	}
		
}
