package sys.mapreduce.distributed.io;

import java.util.List;
import java.util.stream.Collectors;

import sys.storage.rest.RestBlobStorage;
import sys.storage.rest.RestNamenodeClient;
import utils.IP;

public class LocalBlocksBlobStorage extends RestBlobStorage {

	private static final String ownIP = IP.hostAddress();

	public LocalBlocksBlobStorage() {
		super( new FilteredNamenodeClient(), (b,i) -> consistentHash(b, i) );
	}

	//blockWritePolicy that keeps the blocks of all the blobs with the same reduce key in the same datanode...
	//Needed so that only a single datanode will have reduce results for a given key. 
	static int consistentHash( String blob, int block ) {
		int i = blob.lastIndexOf('-');
		String keyPrefix = i >= 0 ? blob.substring(0, i) : blob;
		return keyPrefix.hashCode() >>> 1;
	}
	
	//To leverage locality: when reading a blob, only return its blocks that are stored locally in this particular Datanode.
	static class FilteredNamenodeClient extends RestNamenodeClient {		
		@Override
		public List<String> read(String name) {
			return super.read(name)
					.stream()
					.filter( uri -> uri.contains( ownIP) )
					.collect( Collectors.toList() );
		}
	}
}
