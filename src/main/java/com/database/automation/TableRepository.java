package com.database.automation;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class TableRepository {
	
	static Session session;
	static ResultSet result;
	
	public TableRepository(Session session){
		
		TableRepository.session = session;
	}
	
	
	public static void createTable(String keyspaceName,String tableName)
	{
		System.out.println("-----------------------Create table----------------------");
		StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
				.append(keyspaceName+"."+tableName)
				.append("( id int primary key, name text)")
				.append(";");
		
		ExecuteQuery.run(session, sb);
	}
	
	
	public static void showTableAllData(String keyspaceName,String tableName)
	{
		System.out.println("-------------------Show table Data----------------------");
		StringBuilder sb = new StringBuilder("SELECT * FROM ")
				.append(keyspaceName +"."+ tableName)
				.append(";");
		
		result = ExecuteQuery.run(session, sb);
		
		for(Row row : result)
		{
			System.out.println(String.format("| %d | %s |", row.getInt("id"),row.getString("name")));
		}
		
	}
	
	public static void insertData(String keyspaceName,String tableName)
	{
		System.out.println("-------------------Insert Data----------------------");
		StringBuilder sb = new StringBuilder("INSERT INTO  ")
				.append(keyspaceName +"."+ tableName)
				.append("(id,name) ")
				.append("values(12,'ayushi' );");
		result = ExecuteQuery.run(session, sb);
		
		 sb = new StringBuilder("INSERT INTO  ")
				.append(keyspaceName +"."+ tableName)
				.append("(id,name) ")
				.append("values(121,'gupta' );");
		result = ExecuteQuery.run(session, sb);
		
		 sb = new StringBuilder("INSERT INTO  ")
				.append(keyspaceName +"."+ tableName)
				.append("(id,name) ")
				.append("values(1,'xyzzz' );");
		result = ExecuteQuery.run(session, sb);
		
		 sb = new StringBuilder("INSERT INTO  ")
				.append(keyspaceName +"."+ tableName)
				.append("(id,name) ")
				.append("values(002,'abbbccc' );");
		result = ExecuteQuery.run(session, sb);	
		
	}

	public static void dropTable(String keyspaceName,String tableName)
	{
		System.out.println("------------------Delete table--------------------");
		StringBuilder sb = new StringBuilder("DROP TABLE IF EXISTS ")
				.append(keyspaceName +"."+ tableName);
		
		ExecuteQuery.run(session, sb);
	}
	
	
}
