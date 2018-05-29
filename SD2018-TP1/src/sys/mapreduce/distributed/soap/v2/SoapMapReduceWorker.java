package sys.mapreduce.distributed.soap.v2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jws.WebService;
import javax.xml.ws.Endpoint;

import api.storage.BlobStorage;
import discovery.Discover;
import sys.mapreduce.Jobs;
import sys.mapreduce.Jobs.JavaJob;
import sys.mapreduce.MapReduceJob;
import sys.mapreduce.distributed.io.LocalBlocksBlobStorage;
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
	final BlobStorage onlyLocalBlocks;
	static final Map<String, Job> jobs = new ConcurrentHashMap<>();

	public SoapMapReduceWorker() {
		this.workerId = Random.key64();
		this.storage = new RestBlobStorage();
		this.onlyLocalBlocks = new LocalBlocksBlobStorage();
		
		Endpoint.publish(BASE_URI, this);
		Discover.me(MapReduceWorker.NAME, BASE_URI.replace("0.0.0.0", IP.hostAddress()));
	}
	
	@Override
	public void newJob(String jobClassBlob, String inputPrefix, String outputPrefix, String jobId) {
		JavaJob javaJob = Jobs.newJobInstance(storage, jobClassBlob);
		new Job(jobId, javaJob, onlyLocalBlocks, inputPrefix, outputPrefix, workerId);
	}

	@Override
	public void executeMapPhase(String jobId) {
		Job job = jobs.get( jobId);
		if( job != null)
			job.executeMapPhase();
	}

	@Override
	public void executeReducePhase(String jobId) {
		Job job = jobs.get( jobId);
		if( job != null)
			job.executeReducePhase();		
	}
	
	static class Job extends MapReduceJob {
		final String jobId;
		
		Job( String jobId, JavaJob jobIstance, BlobStorage jobStorage, String inputPrefix, String outputPrefix, String workerId) {
			super(jobIstance, jobStorage, inputPrefix, outputPrefix, workerId);
			this.jobId = jobId;
			jobs.put( jobId, this);
		}
		
		void executeMapPhase() {
			getMapper().execute(inputPrefix).commit();			
		}
		
		void executeReducePhase() {			
			getReducer().execute(outputPrefix + "-map-").commit();
			jobs.remove(jobId);
		}		
	}
}
