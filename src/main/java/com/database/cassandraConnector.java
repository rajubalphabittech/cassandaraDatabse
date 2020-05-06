package com.database;
import com.codahale.metrics.ConsoleReporter.Builder;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;

public class cassandraConnector {

	private Cluster cluster;
	private Session session;
	
	//configure socket options
	SocketOptions options = new SocketOptions()
			.setConnectTimeoutMillis(30000)
			.setReadTimeoutMillis(300000)
			.setTcpNoDelay(true);
	
	public void connect(String node,Integer port) {
		com.datastax.driver.core.Cluster.Builder b = Cluster.builder().addContactPoint(node).withSocketOptions(options) ;
	
		if(port!= null)
			b.withPort(port);
		
		cluster =  b.withoutJMXReporting().build();
		session = cluster.connect();
	}
	
	public Session getSession() {
		return this.session;
	}
	
	public void close() {
		session.close();
		cluster.close();
	}
	
}
