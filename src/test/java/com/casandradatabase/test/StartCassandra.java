package com.casandradatabase.test;

import java.util.List;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import com.database.automation.KeyspaceRepository;
import com.database.automation.TableRepository;
import com.database.automation.cassandraConnector;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class StartCassandra {

	private cassandraConnector client;
	private KeyspaceRepository schema;
	private String node = "127.0.0.1";
	private Integer port = 9042;
	private Session session;
	private String keyspaceName = "demo";
	private String replicationStrategy = "SimpleStrategy";
	private int replicationFactor = 1;
	private String tableName = "person";
	
	@BeforeTest
	public void connect() {
		client = new cassandraConnector();
		client.connect(node, port);
		this.session = client.getSession();
		schema = new KeyspaceRepository(session);
		new TableRepository(session);
		
	}
	
	@Test(priority = 1)
	public void whenCreatingKeyspace() {		
		System.out.println("============create keyspace====================");
		schema.createKeyspace(keyspaceName, replicationStrategy, replicationFactor);
				
		System.out.println("-------------list of keysapces-------------------");
		
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
//		boolean bool = schema.showAllKeysapce(keyspaceName);
		Assert.assertTrue(bool);
	}
	
	@Test(dependsOnMethods = "whenCreatingKeyspace",priority = 2)
	public void creatingTable()	{
	    
		TableRepository.createTable(keyspaceName, tableName);
		
	}
	
	@Test(priority = 3,dependsOnMethods = "creatingTable")
	public void insertData() {
		TableRepository.insertData(keyspaceName, tableName);
		
	}
	
	@Test(dependsOnMethods = "creatingTable",priority = 4)
	public void displayTableData() {
		TableRepository.showTableAllData(keyspaceName, tableName);
		
	}
	
	@Test(dependsOnMethods = "creatingTable",priority = 5)
	public void dropTable() {
		TableRepository.dropTable(keyspaceName, tableName);
		
	}
	
	
	
	@AfterClass
	public void deletingKeyspace() {
		System.out.println("============delete keyspace====================");
		schema.deleteKeyspace(keyspaceName);
		
		System.out.println("-------------list of keysapces-------------------");
		boolean bool = schema.showAllKeysapce(keyspaceName);
		Assert.assertFalse(bool);
		
		
	}
	
}

