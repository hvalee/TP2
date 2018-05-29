package sys.storage.rest;

import static sys.storage.rest.datanode.DatanodeServer.DATANODE;
import static sys.storage.rest.namenode.NamenodeServer.NAMENODE;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import api.storage.BlobStorage;
import api.storage.Datanode;
import api.storage.Namenode;
import discovery.Discover;
import sys.storage.io.BufferedBlobReader;
import sys.storage.io.BufferedBlobWriter;
import utils.Sleep;

public class RestBlobStorage implements BlobStorage {
	
	private static final int BLOCK_SIZE = 1024;

	protected Namenode namenode;
	protected volatile Datanode[] datanodes;
	protected BiFunction<String, Integer, Integer> blockWritePolicy;
	
	final static BiFunction<String, Integer, Integer> SHUFFLE_BLOCKS = (name, block) -> (block + name.hashCode() >>> 1);
	
	public RestBlobStorage() {
		this( Discover.uriOf(NAMENODE), Discover.uriOf( DATANODE), SHUFFLE_BLOCKS );
	}

	public RestBlobStorage( BiFunction<String, Integer, Integer> blockWritePolicy) {
		this( Discover.uriOf(NAMENODE), Discover.uriOf( DATANODE), SHUFFLE_BLOCKS);
	}
	
	protected RestBlobStorage(Namenode namenode, BiFunction<String, Integer, Integer> blockWritePolicy) {
		this.namenode = namenode;
		this.datanodes = new Datanode[] { new RestDatanodeClient( Discover.uriOf( DATANODE)) };
		this.blockWritePolicy = blockWritePolicy;
		new Thread( this::discoverDatanodes ).start();
	}

	private RestBlobStorage(URI namenode, URI datanode, BiFunction<String, Integer, Integer> blockWritePolicy) {
		this.namenode = new RestNamenodeClient(namenode);
		this.datanodes = new Datanode[] { new RestDatanodeClient( datanode ),  };
		this.blockWritePolicy = blockWritePolicy;
		new Thread( this::discoverDatanodes ).start();
	}

	
	@Override
	public List<String> listBlobs(String prefix) {
		return namenode.list(prefix);
	}

	@Override
	public void deleteBlobs(String prefix) {
		namenode.delete(prefix);
	}

	@Override
	public BlobReader readBlob(String name) {
		return new BufferedBlobReader( name, namenode, datanodes[0]);
	}

	@Override
	public BlobWriter blobWriter(String name) {
		return new BufferedBlobWriter( name, namenode, datanodes, BLOCK_SIZE, blockWritePolicy);
	}

	public void discoverDatanodes() {
		final int RETRY_PERIOD = 100;
		
		// keep the uris sorted to have consistency of the indexes across instances...
		Set<URI> uris = new TreeSet<>( this::uriComparator );
		for(;;) {
			if( uris.addAll( Discover.urisOf(DATANODE) ) ) {
				
				//creates a new array of datanode clients (sorted according to their uri).
				datanodes = uris.stream()
						.map( uri -> new RestDatanodeClient( uri ) )
						.collect( Collectors.toList() )
						.toArray( new Datanode[uris.size()]);
			}
			Sleep.ms(RETRY_PERIOD);			
		}
	}

	// The array of nodes needs to be sorted everywhere to
	// implement consistent mapping of blocks to datanodes based on name
	public int uriComparator(URI a, URI b) {
		return a.getHost().compareTo( b.getHost() );
	}
}
