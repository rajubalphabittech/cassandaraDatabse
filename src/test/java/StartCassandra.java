
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.math3.util.MultidimensionalCounter.Iterator;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.database.KeyspaceRepository;
import com.database.cassandraConnector;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class StartCassandra {

	private cassandraConnector client;
	private KeyspaceRepository schema;
	private String node = "127.0.0.1";
	private Integer port = 9042;
	private Session session;
	private String keyspaceName = "person";
	private String replicationStrategy = "SimpleStrategy";
	private int replicationFactor = 1;
	
	@BeforeTest
	public void connect() {
		client = new cassandraConnector();
		client.connect(node, port);
		this.session = client.getSession();
		schema = new KeyspaceRepository(session);
		
	}
	
	@Test
	public void whenCreatingKeyspace() {		
		System.out.println("============craete keyspace====================");
		schema.createKeyspace(keyspaceName, replicationStrategy, replicationFactor);
		
		ResultSet result = session.execute("SELECT * FROM system_schema.keyspaces;");
		
		List<String> keyspacesList = result.all() .stream().map(r -> r.getString(0)).collect(Collectors.toList());
						
		java.util.Iterator<String> iterator = keyspacesList.iterator();
		
		while(iterator.hasNext()) {
			String list =  iterator.next();
			System.out.println(list);
		   if(keyspaceName.equalsIgnoreCase(list))
			Assert.assertEquals(keyspaceName, keyspacesList.get(0));
		}
	}
	
	@AfterTest
	public void deletingKeyspace() {
		System.out.println("============delete keyspace====================");
		schema.deleteKeyspace(keyspaceName);
		
ResultSet result = session.execute("SELECT * FROM system_schema.keyspaces;");
		
		List<String> keyspacesList = result.all() .stream().map(r -> r.getString(0)).collect(Collectors.toList());
						
		java.util.Iterator<String> iterator = keyspacesList.iterator();
		Boolean bool=false ;
		while(iterator.hasNext()) {
			String list =  iterator.next();
			System.out.println(list);
		   if(keyspaceName.equalsIgnoreCase(list)) {
			   bool = true;
			   break;
		   }
		}		
		Assert.assertFalse(bool);
		
		
	}
	
}

