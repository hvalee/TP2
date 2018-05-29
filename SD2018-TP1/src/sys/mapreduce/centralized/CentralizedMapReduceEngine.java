package sys.mapreduce.centralized;


import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import jersey.repackaged.com.google.common.collect.Lists;
import sys.mapreduce.AbstractMapReduceEngine;
import sys.mapreduce.JobMapper;
import sys.mapreduce.JobReducer;
import sys.mapreduce.Jobs;
import sys.mapreduce.Jobs.JavaJob;
import sys.storage.rest.RestBlobStorage;

@SuppressWarnings("rawtypes")
public class CentralizedMapReduceEngine extends AbstractMapReduceEngine {

	public static final String CENTRALIZED = "centralized";

	public CentralizedMapReduceEngine() {
		super( new RestBlobStorage() );
	}

	@Override
	public void executeJob(String jobClassBlob, String inputPrefix, String outputPrefix, int outputPartitionsSize) {
		
		//Creates the mapreducer object for the desired mapreduce program
		JavaJob jobInstance = Jobs.newJobInstance(storage, jobClassBlob);
		
		//Runs the map phase for the given inputprefix; then commits the resulting blobs to storage.
		new JobMapper(jobInstance, storage, inputPrefix, outputPrefix, CENTRALIZED).execute(inputPrefix).commit();

		// Computes the set of keys produced in the map phase
		Set<String> reduceKeyPrefixes = storage.listBlobs(outputPrefix + "-map-").stream()
					.map(blob -> blob.substring(0, blob.lastIndexOf('-')))
					.collect(Collectors.toSet());

		//Partitions the set of keys; the keys of each partition are reduced together and output to the same blob
		AtomicInteger partitionCounter = new AtomicInteger(0);
		Lists.partition(new ArrayList<>(reduceKeyPrefixes), outputPartitionsSize).parallelStream()
				.forEach(partitionKeyList -> {

					String partitionOutputBlob = String.format("%s-part%04d", outputPrefix, partitionCounter.incrementAndGet());
					JobReducer reducer = new JobReducer(jobInstance, storage, inputPrefix, partitionOutputBlob, CENTRALIZED);
					partitionKeyList.forEach(keyPrefix -> {
						reducer.execute(keyPrefix);
					});
					reducer.commit();
				});
		
		// tidy up, cleaning all intermedia blob generated in the map phase
		storage.deleteBlobs(outputPrefix + "-map");
	}
}
