package com.database.automation;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Cluster.Builder;

public class cassandraConnector {

	private Cluster cluster;
	private Session session;
	
	//configure socket options
	SocketOptions options = new SocketOptions()
			.setConnectTimeoutMillis(30000)
			.setReadTimeoutMillis(300000)
			.setTcpNoDelay(true);
	
	public void connect(String node,Integer port) {
		Builder b = Cluster.builder().addContactPoint(node).withSocketOptions(options) ;
	
		if(port!= null)
			b.withPort(port);
		
		cluster =  b.withoutJMXReporting().build();
		session = cluster.connect();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to "+cluster.getClusterName());
		
		for(Host host : metadata.getAllHosts())
			System.out.printf("\nDataCenter : %s; \nHost : %s; \nRack : %s\n",host.getDatacenter(),host.getAddress(),host.getRack());
	}
	
	public Session getSession() {
		return this.session;
	}
	
	public void close() {
		session.close();
		cluster.close();
	}
	
	
}
