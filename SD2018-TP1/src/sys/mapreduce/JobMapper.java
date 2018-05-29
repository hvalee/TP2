package sys.mapreduce;

import static utils.Log.Log;

import java.util.HashMap;
import java.util.Map;

import api.storage.BlobStorage;
import api.storage.BlobStorage.BlobWriter;
import sys.mapreduce.Jobs.JavaJob;
import utils.Base58;
import utils.JSON;

@SuppressWarnings({"unchecked", "rawtypes"})
public class JobMapper extends MapReduceJob {

	public JobMapper( JavaJob jobInstance, BlobStorage storage, String inputPrefix, String outputPrefix, String worker) {		
		super( jobInstance, storage, inputPrefix, outputPrefix, worker );	
		job.instance.setYielder( (key, val) -> jsonValueWriterFor( key ).write(val));
		job.instance.map_init();
	}
		
	public JobMapper execute( String inputPrefix ) {
		Log.finest("-----------------------------------------------");
		storage.listBlobs( inputPrefix ).stream()
			.forEach( blob -> {
				Log.finest(blob);
				storage.readBlob( blob ).forEach( Log::finest );
			});
		
		storage.listBlobs( inputPrefix ).stream()
			.forEach( blob -> {
				storage.readBlob( blob ).forEach( line -> job.instance.map( blob, line ) );
			});
		return this;
	}

	public void commit() {
		job.instance.map_end();
		writers.values().parallelStream().forEach( JsonBlobWriter::close );					
		writers.clear();		
	}
	
	synchronized private JsonBlobWriter jsonValueWriterFor(Object key) {
		JsonBlobWriter res = writers.get( key );
		if( res == null ) {			
			String b58key = Base58.encode( JSON.encode( key ) );
			BlobWriter out = storage.blobWriter( String.format("%s-map-%s-%s", outputPrefix, b58key, worker));
			writers.put(key,  res = new JsonBlobWriter(out));
		}
		return res;
	}	
	
	private Map<Object, JsonBlobWriter> writers = new HashMap<>();
}
