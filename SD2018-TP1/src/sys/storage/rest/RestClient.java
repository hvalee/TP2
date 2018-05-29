package sys.storage.rest;

import java.net.URI;
import java.util.function.Supplier;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import utils.Sleep;

abstract class RestClient {
	protected static final int RETRY_SLEEP = 500;

	private static final int READ_TIMEOUT = 15000;
	private static final int CONNECT_TIMEOUT = 10000;

	protected final URI uri;
	protected final Client client;
	protected final WebTarget target;
	protected final ClientConfig config;



	public class InsecureHostnameVerifier implements HostnameVerifier {
		@Override
		public boolean verify(String hostname, SSLSession session) {
			return true;
		}
	}


	public RestClient(URI uri, String path ) {
		this.uri = uri;
		this.config = new ClientConfig();
		this.config.property(ClientProperties.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
		this.config.property(ClientProperties.READ_TIMEOUT, READ_TIMEOUT);		
		this.client = ClientBuilder.newBuilder().hostnameVerifier( new InsecureHostnameVerifier() ).build();
		this.target = this.client.target(uri).path( path );
	}

	// higher order function to retry forever a call of a void return type,
	// until it succeeds and returns to break the loop
	protected void reTry( Runnable func) {
		for(;;)
			try {
				func.run();	
				return;
			} catch( ProcessingException x ) {
				x.printStackTrace();
				Sleep.ms( RETRY_SLEEP);
			}
	}

	// higher order function to retry forever a call until it succeeds 
	// and return an object of some type T to break the loop
	protected <T> T reTry( Supplier<T> func) {
		for(;;)
			try {
				return func.get();			
			} catch( ProcessingException x ) {
				x.printStackTrace();
				Sleep.ms( RETRY_SLEEP);
			}
	}

	// Get the actual response, when the status matches what was expected, otherwise return a default value
	protected <T> T responseContents( Status expecting, Response r, T orElse, GenericType<T> gtype) {
		try {
			if( expecting.getStatusCode() == r.getStatus())
				return r.readEntity( gtype );
			else 
				return orElse;			
		} finally {
			r.close();
		}
	}

	// Checks if response status matches what was expected, throws exception otherwise
	protected void verifyResponse( Status expecting, Response r) {
		try {
			int status = r.getStatus();
			if( status != expecting.getStatusCode() )
				throw new RuntimeException(String.format("FAILED...[%s]", status) );
		} finally {
			r.close();
		}
	}

	@Override
	public String toString() {
		return uri.toString();
	}
}
