package sys.mapreduce.distributed.kafka.v2;

import static sys.mapreduce.distributed.kafka.v2.MapReduceEvents.ACK_JOB;
import static sys.mapreduce.distributed.kafka.v2.MapReduceEvents.FINISHED_MAP_PHASE;
import static sys.mapreduce.distributed.kafka.v2.MapReduceEvents.FINISHED_REDUCE_PHASE;
import static sys.mapreduce.distributed.kafka.v2.MapReduceEvents.MAPREDUCE;
import static sys.mapreduce.distributed.kafka.v2.MapReduceEvents.WORKER_READY;
import static sys.mapreduce.distributed.kafka.v2.MapReduceEvents.endJob;
import static sys.mapreduce.distributed.kafka.v2.MapReduceEvents.jobTopic;
import static sys.mapreduce.distributed.kafka.v2.MapReduceEvents.newJob;
import static sys.mapreduce.distributed.kafka.v2.MapReduceEvents.ready4Acks;
import static sys.mapreduce.distributed.kafka.v2.MapReduceEvents.startReducePhase;
import static sys.mapreduce.distributed.kafka.v2.MapReduceEvents.topics;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import com.google.common.collect.Sets;

import api.storage.BlobStorage;
import sys.mapreduce.distributed.kafka.KafkaClient;
import utils.Random;
import utils.Sleep;

public class KafkaMapReduceMaster extends KafkaClient {

	private static final long TIMEOUT = 250;
	
	final BlobStorage storage;
	final Set<String> AvailableWorkerIDs = Sets.newConcurrentHashSet();
	
	public KafkaMapReduceMaster(BlobStorage storage) {
		this.storage = storage;
		new Thread( this::init ).start();
	}

	
	// Computenodes announce themselves at startup. We want to collect their Ids. Note: Nodes recover after failure...
	public void init() {
		try (Producer<String,String> producer = new KafkaProducer<>(pProps); Consumer<String, String> consumer = new KafkaConsumer<>(cProps)) {
			producer.send(MapReduceEvents.ready4Jobs("master"));
			consumer.subscribe(topics(MAPREDUCE));
			producer.close();
			for(;;) {
				consumer.poll( TIMEOUT).forEach( r -> {
					String id = r.value();
					if( r.key().equals( WORKER_READY.name()) && !id.equals("master"))
						AvailableWorkerIDs.add( r.value() );
				});
			}
		}
	}
		
	//Waits for events with a given key from all the given expected respondants...
	private Set<String> awaitAll( Consumer<String, String> consumer, MapReduceEvents key, Set<String> whom) {
		Set<String> respondents = new HashSet<>() ;
		Set<String> pending = new HashSet<>( whom );
		do {
			for( ConsumerRecord<String, String> r : consumer.poll(TIMEOUT)) {
				System.out.printf("topic = %s, key = %s, value = %s, offset=%s%n", r.topic(), r.key(), r.value(), r.offset());
				if( r.key().equals( key.name() )) {
					String workerId = r.value();
					pending.remove( workerId );
					respondents.add( workerId );
				}
			};
		} while( pending.size() > 0 );
		return respondents;
	}
	
	public KafkaMapReduceMaster executeJob(String jobClassBlob, String inputPrefix, String outputPrefix, int outputPartitionsSize) {

		while( AvailableWorkerIDs.isEmpty() );
			Sleep.ms(50);
		
		String jobId = Random.key64();

		cProps.put(ConsumerConfig.GROUP_ID_CONFIG, Random.key64() );
		try (Producer<String,String> producer = new KafkaProducer<>(pProps); Consumer<String, String> consumer = new KafkaConsumer<>(cProps)) {

			producer.send( ready4Acks(jobId));
			consumer.subscribe(Arrays.asList(jobTopic(jobId)));

			broadcast(producer, newJob(jobId, jobClassBlob, inputPrefix, outputPrefix));
			Set<String> jobWorkers = awaitAll(consumer, ACK_JOB,  AvailableWorkerIDs );

			jobWorkers = awaitAll(consumer, FINISHED_MAP_PHASE, jobWorkers );

			broadcast(producer, startReducePhase(jobId));
			
			awaitAll(consumer, FINISHED_REDUCE_PHASE, jobWorkers );

			broadcast(producer, endJob(jobId));

			storage.deleteBlobs(outputPrefix + "-map");
		}
		return this;
	}
}
