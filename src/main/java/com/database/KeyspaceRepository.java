package com.database;

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
	
}
