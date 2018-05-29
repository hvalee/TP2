package sys.storage.rest.datanode;

import static utils.Log.Log;

import java.net.URI;
import java.util.logging.Level;

import javax.net.ssl.SSLContext;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import api.storage.Datanode;
import discovery.Discover;
import sys.mapreduce.centralized.CentralizedMapReduceEngine;
import utils.IP;
import utils.Props;

public class DatanodeServer {
	public static final int DATANODE_PORT = 9999;
	public static final String DATANODE = "Datanode";
	
	private static final String PROPS_FILENAME = "/props/sd2018-tp1.props";
	private static final String MAPREDUCE_WORKER_PROP = "mapreduce-worker";

	
	public static void main(String[] args) throws Exception {
		System.setProperty("java.net.preferIPv4Stack", "true");
		
		Log.setLevel(Level.FINER);

		String ip = IP.hostAddress();
		String serverURI = String.format("https://%s:%s/", ip, DATANODE_PORT);

		ResourceConfig config = new ResourceConfig();
		config.register(new DatanodeResources(serverURI));

		JdkHttpServerFactory.createHttpServer(URI.create(serverURI.replace(ip, "0.0.0.0")), config, SSLContext.getDefault());

		System.err.printf("Datanode Server ready @ %s%s\n", serverURI, Datanode.PATH.substring(1));

		//Allow the Datanode to be discovered...
		new Thread( () -> Discover.me(DATANODE, serverURI) ).start();

		//Select the MapReduce worker class from the .props file and launch it
		Props.parseFile(PROPS_FILENAME);
		String mrWorkerClass = Props.get(MAPREDUCE_WORKER_PROP, CentralizedMapReduceEngine.class.toString());
		Class.forName(mrWorkerClass).newInstance(); 
	}
}
