package sys.mapreduce.distributed.soap.v1;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jws.WebService;
import javax.xml.ws.Endpoint;

import api.storage.BlobStorage;
import discovery.Discover;
import sys.mapreduce.Jobs;
import sys.mapreduce.Jobs.JavaJob;
import sys.mapreduce.MapReduceJob;
import sys.storage.rest.RestBlobStorage;
import utils.IP;
import utils.Random;

@SuppressWarnings({"rawtypes"})
@WebService(serviceName = MapReduceWorker.NAME, targetNamespace = MapReduceWorker.NAMESPACE, endpointInterface = MapReduceWorker.INTERFACE)
public class SoapMapReduceWorker implements MapReduceWorker {

	private static final int MAPREDUCE_WORKER_PORT = 6666;
	private static String BASE_URI = String.format("http://0.0.0.0:%d%s", MAPREDUCE_WORKER_PORT, MapReduceWorker.PATH);

	final String workerId;
	final BlobStorage storage;
	static final Map<String, Job> jobs = new ConcurrentHashMap<>();

	public SoapMapReduceWorker() {
		this.workerId = Random.key64();
		this.storage = new RestBlobStorage();
		Endpoint.publish(BASE_URI, this);
		Discover.me(MapReduceWorker.NAME, BASE_URI.replace("0.0.0.0", IP.hostAddress()));
	}
	
	@Override
	public void newJob(String jobClassBlob, String inputPrefix, String outputPrefix, String jobId) {
		JavaJob javaJob = Jobs.newJobInstance(storage, jobClassBlob);
		new Job(jobId, javaJob, storage, jobClassBlob, inputPrefix, outputPrefix, workerId);
	}

	@Override
	public void doMapTask(String jobId, String inputPrefix) {
		Job job = jobs.get( jobId);
		if( job != null)
			job.doMapTask(inputPrefix);
	}

	@Override
	public void commitIntermediaResultsToStorage(String jobId) {
		Job job = jobs.get( jobId);
		if( job != null)
			job.commitIntermediaResultsToStorage();		
	}

	@Override
	public void doReduceTask(String jobId, String inputPrefix) {
		Job job = jobs.get( jobId);
		if( job != null)
			job.doReduceTask( inputPrefix );
	}

	@Override
	public void endJob(String jobId) {
		Job job = jobs.get( jobId);
		if( job != null)
			job.dispose();
	}
	
	static class Job extends MapReduceJob {
		final String jobId;
		
		Job( String jobId, JavaJob jobInstance, BlobStorage storage, String jobClassBlob, String inputPrefix, String outputPrefix, String workerId) {
			super( jobInstance, storage, inputPrefix, outputPrefix, workerId);
			this.jobId = jobId;
			jobs.put( jobId, this);
		}
		
		synchronized void doMapTask( String inputPrefix) {			
			getMapper().execute(inputPrefix);
		}

		synchronized void commitIntermediaResultsToStorage() {			
			getMapper().commit();
		}
		
		synchronized void doReduceTask( String keyPrefix ) {
			getReducer().execute(keyPrefix);
		}

		synchronized void dispose() {
			getReducer().commit();
			jobs.remove( jobId );
		}		
	}
}
