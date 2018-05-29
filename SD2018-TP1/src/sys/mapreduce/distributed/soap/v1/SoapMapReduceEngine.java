package sys.mapreduce.distributed.soap.v1;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sys.mapreduce.distributed.soap.AbstractSoapMapReduceEngine;
import utils.Random;
import utils.Sleep;

// Does a distributed MapReduce job by dispatching map and reduce tasks
// to the available workers.
// Does not exploit data locality, meaning that any worker can receive any task

// Note: This implementation cannot incorporate new workers once a job has started...

public class SoapMapReduceEngine extends AbstractSoapMapReduceEngine<MapReduceWorker> {

	public SoapMapReduceEngine() {
		super( new MapReduceWorkerClientFactory() );
	}
	
	@Override
	public void executeJob( String jobClassBlob, String inputPrefix, String outputPrefix, int outputPartitionsSize ) {
		
		//Wait for at least one worker
		while( super.availableWorkers().isEmpty() )
			Sleep.ms(100);
		
		List<MapReduceWorker> workers = super.availableWorkers();
		
		String jobId = Random.key128();
		
		//Tell the workers to prepare and start a new mapreduce job 
		workers.parallelStream().forEach( worker -> {
			worker.newJob(jobClassBlob, inputPrefix, outputPrefix, jobId);
		});
		
		// Each blob in the prefix corresponds to an individual map Task
		// Select a random worker for each map task
		storage.listBlobs( inputPrefix ).parallelStream().forEach( inputBlob  -> {
			randomWorker(workers).doMapTask(jobId, inputBlob);
		});
		
		//
		workers.parallelStream().forEach( worker -> {
			worker.commitIntermediaResultsToStorage(jobId);
		});
		
		Set<String> reduceKeyPrefixes = storage.listBlobs(outputPrefix + "-map-").stream()
			.map( blob -> blob.substring( 0, blob.lastIndexOf('-')))
			.collect( Collectors.toSet() );
		
		reduceKeyPrefixes.parallelStream().forEach( keyPrefix -> {
			randomWorker(workers).doReduceTask(jobId, keyPrefix);			
		});			

		workers.parallelStream().forEach( worker -> {
			worker.endJob(jobId);
		});
		
		storage.deleteBlobs(outputPrefix+"-map-");
	}
}
