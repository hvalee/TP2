package sys.storage.rest.namenode;

import static utils.Log.Log;

import java.net.URI;
import java.util.logging.Level;

import javax.net.ssl.SSLContext;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import api.storage.Namenode;
import discovery.Discover;
import utils.IP;


public class NamenodeServer {
	public static final String NAMENODE = "Namenode";

	public static final int NAMENODE_PORT = 7777;

	public static void main(String[] args) throws Exception {
		System.setProperty("java.net.preferIPv4Stack", "true");

		Log.setLevel( Level.FINER );

		String ip = IP.hostAddress();
		String serverURI = String.format("https://%s:%s/", ip, NAMENODE_PORT);
		
		ResourceConfig config = new ResourceConfig();
		config.register( new NamenodeResources() );
		
		JdkHttpServerFactory.createHttpServer( URI.create(serverURI.replace(ip, "0.0.0.0")), config, SSLContext.getDefault());

		Log.fine(String.format("Namenode Server ready @ %s%s\n",  serverURI, Namenode.PATH.substring(1)));
		
		Discover.me(NAMENODE, serverURI);
	}
}
