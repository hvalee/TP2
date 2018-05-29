package sys.mapreduce;

import api.storage.BlobStorage;
import sys.mapreduce.Jobs.JavaJob;

@SuppressWarnings("rawtypes")
public abstract class MapReduceJob {

	protected final JavaJob job;
	protected final String worker;
	protected final BlobStorage storage;

	protected final String inputPrefix;
	protected final String outputPrefix;

	protected MapReduceJob(JavaJob jobInstance, BlobStorage storage, String inputPrefix, String outputPrefix, String worker) {
		this.worker = worker;
		this.job = jobInstance;
		this.storage = storage;
		this.inputPrefix = inputPrefix;
		this.outputPrefix = outputPrefix;
	}
	
	synchronized protected JobMapper getMapper() {
		if (mapper == null)
			mapper = new JobMapper(job, storage, inputPrefix, outputPrefix, worker);
		return mapper;
	}

	synchronized protected JobReducer getReducer() {
		if (reducer == null)
			reducer = new JobReducer(job, storage, inputPrefix, outputPrefix, worker);
		return reducer;
	}

	private JobMapper mapper;
	private JobReducer reducer;
}
