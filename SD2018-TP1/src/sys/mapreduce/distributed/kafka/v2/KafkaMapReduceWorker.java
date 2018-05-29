package sys.mapreduce.distributed.kafka.v2;

import static sys.mapreduce.distributed.kafka.v2.MapReduceEvents.MAPREDUCE;
import static sys.mapreduce.distributed.kafka.v2.MapReduceEvents.ackJob;
import static sys.mapreduce.distributed.kafka.v2.MapReduceEvents.ackMapPhase;
import static sys.mapreduce.distributed.kafka.v2.MapReduceEvents.ackReducePhase;
import static sys.mapreduce.distributed.kafka.v2.MapReduceEvents.ready4Jobs;
import static sys.mapreduce.distributed.kafka.v2.MapReduceEvents.topics;
import static utils.Log.Log;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import api.storage.BlobStorage;
import sys.mapreduce.Jobs;
import sys.mapreduce.Jobs.JavaJob;
import sys.mapreduce.MapReduceJob;
import sys.mapreduce.distributed.io.LocalBlocksBlobStorage;
import sys.mapreduce.distributed.kafka.KafkaClient;
import sys.mapreduce.distributed.kafka.v2.MapReduceEvents.JobData;
import sys.storage.rest.RestBlobStorage;
import utils.Random;

@SuppressWarnings({"rawtypes"})
public class KafkaMapReduceWorker extends KafkaClient {

	private static final long TIMEOUT = 1000;

	BlobStorage fullStorage;
	BlobStorage localBlockstorage;
	
	final String workerId;
	final Producer<String, String> producer;
	final Map<String, Job> jobs = new ConcurrentHashMap<>();

	public KafkaMapReduceWorker() {
		this.workerId = Random.key64();
		this.producer = new KafkaProducer<>(pProps);
		this.producer.send( ready4Jobs( workerId ) );		
		new Thread( this::handleJobRequests ).start();
	}
	
	void handleJobRequests() {
		this.fullStorage = new RestBlobStorage();
		this.localBlockstorage = new LocalBlocksBlobStorage();
		
		cProps.put(ConsumerConfig.GROUP_ID_CONFIG, Random.key64() );	
		try (Consumer<String, String> jobListener = new KafkaConsumer<>(cProps)) {
			jobListener.subscribe(topics(MAPREDUCE));

			while (true) {
				jobListener.poll(TIMEOUT).forEach(r -> {
					System.out.printf("#### topic = %s, key = %s, value = %s, offset=%s%n", r.topic(), r.key(), r.value(), r.offset());
					Job job;
					switch( MapReduceEvents.valueOf(r.key()) ) {
						case NEW_JOB: new Job( new JobData(r) ).doMapPhase() ; 
							break;
						case START_REDUCE_PHASE: 
							job = jobs.get( r.value() );
							if( job != null )
								job.doReduceTask();
							break;
						case END_JOB: 
							job = jobs.get( r.value() );
							if( job != null )
								job.dispose(); 
						default:
						break;
					}
				});
			}
		}
	}
	
	JavaJob getJob( String jobClassBlob ) {
		return Jobs.newJobInstance(fullStorage, jobClassBlob);
	}
	
	class Job extends MapReduceJob {
		final String jobId;
		boolean completed = false; 
		
		Job( JobData info ) {
			super( getJob(info.jobClassBlob), fullStorage, info.inputPrefix, info.outputPrefix, workerId);
			this.jobId = info.jobId;
			jobs.put( jobId, this);
		}
		
		synchronized void doMapPhase() {			
			producer.send( ackJob( jobId, workerId ));
			getMapper().execute(inputPrefix).commit();
			producer.send( ackMapPhase( jobId, workerId ));
		}

		synchronized void doReduceTask() {

			String mapPhasePrefix = outputPrefix +"-map";			
			
			Log.finest("-----------------------------------------------");
			storage.listBlobs( mapPhasePrefix ).forEach( Log::finest );
			
			Set<String> reduceKeyPrefixes = storage.listBlobs(mapPhasePrefix ).stream()
				 .map( blob -> blob.substring( 0, blob.lastIndexOf('-')))
				 .collect( Collectors.toSet() );

			reduceKeyPrefixes.forEach( reduceKeyPrefix -> {
				getReducer().execute(reduceKeyPrefix);				
			});
			getReducer().commit();
			producer.send( ackReducePhase( jobId, workerId ));
		}

		void dispose() {
			jobs.remove( jobId );
		}		
	}
}
