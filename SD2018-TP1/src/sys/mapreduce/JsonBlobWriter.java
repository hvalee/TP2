package sys.mapreduce;

import api.storage.BlobStorage.BlobWriter;
import utils.JSON;

/*
 * 
 * Wraps a BlobWriter to allow writing one json object per blob line.
 * 
 */
final public class JsonBlobWriter {
	
	private final BlobWriter out;
	
	public JsonBlobWriter( BlobWriter out ) {
		this.out = out;
	}
	
	public <T> void write( T obj ) {	
		out.writeLine( JSON.encode(obj) );
	}
		
	public void close() {
		out.close();
	}
}
