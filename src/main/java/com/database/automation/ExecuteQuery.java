package com.database.automation;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class ExecuteQuery {
	
	public static ResultSet run(Session session, StringBuilder queryToRun) {
		
	   String query = queryToRun.toString();
	   System.out.println(query);
	   ResultSet result = session.execute(query);
	   return result;
	 
	}

}
