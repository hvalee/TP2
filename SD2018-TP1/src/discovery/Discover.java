package discovery;

import static utils.Log.Log;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Discover {
	private static final int MAX_DATAGRAM_SIZE = 65536;

	private static final int RETRY_INTERVAL = 100;
	private static final int DISCOVERY_TIMEOUT = 1000;
	private static final InetSocketAddress DISCOVERY_ADDR = new InetSocketAddress("226.226.226.226", 3333);

	// Used to allow a service to be discovered.
	// Simply waits on a multicast socket and port
	// for requests that match the name of the service
	// replies back to the source of the request using unicast 

	public static void me(String key, String url) {
		byte[] replyData = url.getBytes();

		Log.finest("Discovery server listening on: " + DISCOVERY_ADDR);
		try (MulticastSocket ms = new MulticastSocket(DISCOVERY_ADDR.getPort())) {
			ms.joinGroup(DISCOVERY_ADDR.getAddress());
			for (;;) {
				DatagramPacket pkt = new DatagramPacket(new byte[MAX_DATAGRAM_SIZE], MAX_DATAGRAM_SIZE);
				ms.receive(pkt);
				String query = new String(pkt.getData(), 0, pkt.getLength());
				if (query.equalsIgnoreCase(key))
					ms.send(new DatagramPacket(replyData, replyData.length, pkt.getSocketAddress()));
				else {
					String msg = "Ignoring discovery request for:" + query;
					Log.finest(msg);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// Spends the alloted timeout to find the uris of the given name, or when enough have been found
	public static Set<URI> search(String query, int needed) {
		byte[] queryData = query.getBytes();
		DatagramPacket request = new DatagramPacket(queryData, queryData.length, DISCOVERY_ADDR);
		
		Set<URI> results = new HashSet<>();
		long deadline = System.currentTimeMillis() + DISCOVERY_TIMEOUT;

		DatagramPacket reply = new DatagramPacket(new byte[MAX_DATAGRAM_SIZE], MAX_DATAGRAM_SIZE);

		try (DatagramSocket ds = new DatagramSocket()) {
			ds.setSoTimeout(RETRY_INTERVAL);
			while (System.currentTimeMillis() < deadline && results.size() < needed) {
				ds.send( request );
				try {
					while( true) {
						reply.setLength(MAX_DATAGRAM_SIZE);
						ds.receive(reply);
						String uri = new String(reply.getData(), 0, reply.getLength());

						Log.finest(String.format("Discovered [%s] at: %s", query, uri));
						results.add( URI.create( uri ));
					}
				} catch (SocketTimeoutException e) {
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if( results.isEmpty() )
			Log.finest(String.format("No discoveries after timeout [%s ms]", DISCOVERY_TIMEOUT));
		
		return results ;
	}

	// Does not return until the uri of a given service is found
	public static URI uriOf(String name) {
		return  urisOf(name).get(0);
	}

	
	// Does not return until at least one uri for the given service is found
	public static List<URI> urisOf(String name) {
		Set<URI> uris;
		while ( (uris = Discover.search(name, 1)).isEmpty())
			;
		return new ArrayList<>( uris );
	}
}
