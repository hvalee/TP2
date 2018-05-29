package sys.mapreduce.distributed.soap.v2;

import java.util.List;

import sys.mapreduce.distributed.soap.AbstractSoapMapReduceEngine;
import utils.Random;
import utils.Sleep;

// Does a distributed MapReduce job by dispatching map and reduce tasks
// to all the available workers.
// Exploits data locality by having each worker only process the blocks it stores locally
// To that end, the blobstorage policy ensures that the map phase results 
// for each emitted key are written to the same datanode. 

//Note: This implementation cannot incorporate new workers once a job has started...

public class SoapMapReduceEngine extends AbstractSoapMapReduceEngine<MapReduceWorker> {


	public SoapMapReduceEngine() {
		super( new MapReduceWorkerClientFactory() );
	}
	
	@Override
	public void executeJob( String jobClassBlob, String inputPrefix, String outputPrefix, int outputPartitionsSize ) {
		
		//Wait for at least one worker
		while( super.availableWorkers().isEmpty() )
			Sleep.ms(100);
		
		//Use a private copy of the available workers
		List<MapReduceWorker> workers = super.availableWorkers();
		
		String jobId = Random.key128();
		
		workers.parallelStream().forEach( worker -> {
			worker.newJob(jobClassBlob, inputPrefix, outputPrefix, jobId);
		});

		// Ask all workers to execute map for all the blobs matching the input prefix of the job
		// and commit the intermediate results when finished
		//
		// For each blob,  each worker will only process its own blocks
		workers.parallelStream().forEach( worker -> {
			worker.executeMapPhase(jobId);
		});
		
		// Ask all workers to execute reduce for all the blobs matching the input prefix of the job
		// and commit the final results when finished
		//
		// For each blob,  each worker will only process its own blocks. To ensure for each key
		// the reduce phase only produces one result it exploits the proper block storage policy...
		workers.parallelStream().forEach( worker -> {
			worker.executeReducePhase(jobId);
		});

		//delete intermediate blobs produced in the map phase.
		storage.deleteBlobs(outputPrefix+"-map-");		
	}
}
