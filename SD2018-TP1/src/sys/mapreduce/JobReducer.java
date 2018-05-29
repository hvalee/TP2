package sys.mapreduce;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import api.storage.BlobStorage;
import api.storage.BlobStorage.BlobWriter;
import jersey.repackaged.com.google.common.collect.Iterators;
import sys.mapreduce.Jobs.JavaJob;
import utils.Base58;
import utils.JSON;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class JobReducer extends MapReduceJob {

	public JobReducer(JavaJob jobInstance, BlobStorage storage, String inputPrefix, String outputPrefix, String worker) {
		super(jobInstance, storage, inputPrefix, outputPrefix, worker);
		
		// when the MapReduce program outputs/yields a (key, value) tuple, write it as line of the output blob
		job.instance.setYielder((k, v) -> writerFor(outputPrefix).writeLine(k + "\t" + v));
		job.instance.reduce_init();
	}

	// Apply reduce by iterating all the values of the intermediate blobs of the key 
	// The key is encoded in the keyPrefix (combination of Base58 and JSON formats) 
	public JobReducer execute(String keyPrefix) {
				
		String encodedKey = keyPrefix.substring(keyPrefix.lastIndexOf('-') + 1);
		Object key = JSON.decode(Base58.decode(encodedKey), job.reducerKeyType());

		Iterator<JsonDecoder> valuesIterators = storage.listBlobs(keyPrefix).stream()
				.map(name -> new JsonDecoder(storage.readBlob(name).iterator(), job.reducerValueType()))
				.collect(Collectors.toList()).iterator();
	
		Iterator<?> values = Iterators.concat(valuesIterators);
		if( values.hasNext())
			job.instance.reduce(key, () -> values);
		return this;
	}

	// Commit all the output blobs (one per processed key) to storage.
	public void commit() {
		job.instance.reduce_end();
		writers.values().parallelStream().forEach(BlobWriter::close);
		writers.clear();
	}

	synchronized private BlobWriter writerFor(String output) {
		BlobWriter res = writers.get(output);
		if (res == null)
			writers.put(output, res = storage.blobWriter(output + "-" + worker));
		return res;
	}

	private Map<Object, BlobWriter> writers = new HashMap<>();
}
