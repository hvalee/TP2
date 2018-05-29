package sys.mapreduce;

import api.storage.BlobStorage;

abstract public class AbstractMapReduceEngine implements MapReduceEngine {

	protected final BlobStorage storage;
	protected AbstractMapReduceEngine( BlobStorage storage ) {
		this.storage = storage;
	}
}
