package com.database.automation;

import java.util.List;
import java.util.stream.Collectors;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class KeyspaceRepository {
	
	Session session;
	public KeyspaceRepository(Session session){
		
		this.session=session;
	}
	

	public void createKeyspace(String keyspaceName,String replicationStrategy,int replicationFactor)
	{
		StringBuilder sb = new StringBuilder("CREATE KEYSPACE ")
				.append(keyspaceName)
				.append(" with replication = {")
				.append("'class':'").append(replicationStrategy)
				.append("','replication_factor':").append(replicationFactor)
				.append("};");
		String query =sb.toString();
		System.out.println(query);
		session.execute(query);
		
	}
	
	public void deleteKeyspace(String keyspaceName) {
		
		StringBuilder sb = new StringBuilder("DROP KEYSPACE IF EXISTS ")
				.append(keyspaceName +";");
		String query =sb.toString();
		System.out.println(query);
		session.execute(query);
	}
	
	public boolean showAllKeysapce(String keyspaceName)	{
		
	
		ResultSet result = session.execute("SELECT * FROM system_schema.keyspaces;");
		
		List<String> keyspacesList = result.all() .stream().map(r -> r.getString(0)).collect(Collectors.toList());
		
		java.util.Iterator<String> iterator = keyspacesList.iterator();
		
		Boolean bool=false ;
		while(iterator.hasNext()) {
			
			String list =  iterator.next();
			System.out.println(list);
			if(keyspaceName.equalsIgnoreCase(list)) {
				   bool = true;
		    }
					
	    }
		return bool;
   }
}
