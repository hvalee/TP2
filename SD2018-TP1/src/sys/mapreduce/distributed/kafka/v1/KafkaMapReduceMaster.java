package sys.mapreduce.distributed.kafka.v1;

import static sys.mapreduce.distributed.kafka.v1.MapReduceEvents.ACK_FINISH_MAP_PHASE;
import static sys.mapreduce.distributed.kafka.v1.MapReduceEvents.ACK_FINISH_REDUCE_PHASE;
import static sys.mapreduce.distributed.kafka.v1.MapReduceEvents.ACK_MAP_TASK;
import static sys.mapreduce.distributed.kafka.v1.MapReduceEvents.ACK_REDUCE_TASK;
import static sys.mapreduce.distributed.kafka.v1.MapReduceEvents.acksTopic;
import static sys.mapreduce.distributed.kafka.v1.MapReduceEvents.endJob;
import static sys.mapreduce.distributed.kafka.v1.MapReduceEvents.finishMapPhase;
import static sys.mapreduce.distributed.kafka.v1.MapReduceEvents.finishReducePhase;
import static sys.mapreduce.distributed.kafka.v1.MapReduceEvents.mapTask;
import static sys.mapreduce.distributed.kafka.v1.MapReduceEvents.newJob;
import static sys.mapreduce.distributed.kafka.v1.MapReduceEvents.ready4Acks;
import static sys.mapreduce.distributed.kafka.v1.MapReduceEvents.reduceTask;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;

import api.storage.BlobStorage;
import sys.mapreduce.distributed.kafka.KafkaClient;
import sys.mapreduce.distributed.kafka.v1.MapReduceEvents.AckEvent;
import utils.Random;

public class KafkaMapReduceMaster extends KafkaClient {

	private static final long TIMEOUT = 100;
	
	final BlobStorage storage;

	public KafkaMapReduceMaster(BlobStorage storage) {
		this.storage = storage;
	}

	AtomicLong taskIdCounter = new AtomicLong(0);
	String newTaskId(String prefix) {
		return String.format("%s%04d", prefix, taskIdCounter.getAndIncrement());
	}
	
	private Set<String> awaitACKs( Consumer<String, String> consumer, MapReduceEvents event, Set<String> pending) {
		Set<String> workers = new HashSet<>();
		do {
			consumer.poll(TIMEOUT).forEach( r -> {
				System.out.printf("topic = %s, key = %s, value = %s, offset=%s%n", r.topic(), r.key(), r.value(), r.offset());
				if( r.key().equals( event.name() )) {
					AckEvent t = new AckEvent(r);
					pending.remove( t.taskId );
					workers.add( t.workerId );
				}				
			});
		} while( pending.size() > 0 );
		return workers;
	}
	
	public KafkaMapReduceMaster executeJob(String jobClassBlob, String inputPrefix, String outputPrefix, int outputPartitionsSize) {

		String jobId = Random.key64();
		Set<String> pendingTasks = new HashSet<>();

		pProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, ValuePartitioner.class.getName());
		
		try (Producer<String,String> producer = new KafkaProducer<>(pProps); Consumer<String, String> consumer = new KafkaConsumer<>(cProps)) {

			producer.send( ready4Acks(jobId));
			consumer.subscribe(Arrays.asList(acksTopic(jobId)));

			broadcast(producer, newJob(jobId, jobClassBlob, inputPrefix, outputPrefix));

			storage.listBlobs(inputPrefix).stream().forEach(blob -> {
				String taskId =  newTaskId("map-");
				anycast(producer, mapTask(jobId, taskId, blob));
				pendingTasks.add( taskId);
			});
			
			Set<String> workerIds = awaitACKs(consumer, ACK_MAP_TASK, pendingTasks);
					
			broadcast( producer, finishMapPhase(jobId));
						
			awaitACKs(consumer, ACK_FINISH_MAP_PHASE, workerIds);
			
			String mapPhasePrefix = outputPrefix +"-map";			
			Set<String> reduceKeyPrefixes = storage.listBlobs(mapPhasePrefix ).stream()
				 .map( blob -> blob.substring( 0, blob.lastIndexOf('-')))
				 .collect( Collectors.toSet() );

			reduceKeyPrefixes.forEach( keyPrefix -> {
				String taskId = newTaskId("reduce-");
				anycast(producer, reduceTask(jobId, taskId, keyPrefix));
				pendingTasks.add( taskId);
			});
			 
			workerIds = awaitACKs(consumer, ACK_REDUCE_TASK, pendingTasks);
				
			broadcast(producer, finishReducePhase(jobId));
							
			awaitACKs(consumer, ACK_FINISH_REDUCE_PHASE, workerIds);

			broadcast(producer, endJob(jobId) );

			storage.deleteBlobs(mapPhasePrefix);
		}
		return this;
	}
	
	
	static public class ValuePartitioner extends DefaultPartitioner {
		int numPartitions = -1;
		
		@Override
		public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
			if( numPartitions < 0 )
				numPartitions = cluster.partitionCountForTopic(topic);		
			return (Arrays.hashCode(valueBytes) >>> 1) % numPartitions;
		}
	}

}
