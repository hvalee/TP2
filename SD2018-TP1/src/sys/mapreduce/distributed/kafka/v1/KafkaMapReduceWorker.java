package sys.mapreduce.distributed.kafka.v1;

import static sys.mapreduce.distributed.kafka.v1.MapReduceEvents.MAPREDUCE;
import static sys.mapreduce.distributed.kafka.v1.MapReduceEvents.ackCommitMapResults;
import static sys.mapreduce.distributed.kafka.v1.MapReduceEvents.ackCommitReduceResults;
import static sys.mapreduce.distributed.kafka.v1.MapReduceEvents.ackMapTask;
import static sys.mapreduce.distributed.kafka.v1.MapReduceEvents.ackReduceTask;
import static sys.mapreduce.distributed.kafka.v1.MapReduceEvents.ready4Jobs;
import static sys.mapreduce.distributed.kafka.v1.MapReduceEvents.ready4Tasks;
import static sys.mapreduce.distributed.kafka.v1.MapReduceEvents.tasksTopic;
import static sys.mapreduce.distributed.kafka.v1.MapReduceEvents.topics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import api.storage.BlobStorage;
import sys.mapreduce.Jobs;
import sys.mapreduce.Jobs.JavaJob;
import sys.mapreduce.MapReduceJob;
import sys.mapreduce.distributed.kafka.KafkaClient;
import sys.mapreduce.distributed.kafka.v1.MapReduceEvents.JobData;
import sys.mapreduce.distributed.kafka.v1.MapReduceEvents.TaskData;
import sys.storage.rest.RestBlobStorage;
import utils.Random;
import utils.Sleep;

@SuppressWarnings({"rawtypes"})
public class KafkaMapReduceWorker extends KafkaClient {

	private static final long TIMEOUT = 1000;

	BlobStorage storage;

	final String workerId;
	final Producer<String, String> producer;
	final Map<String, Job> jobs = new ConcurrentHashMap<>();

	public KafkaMapReduceWorker() {
		this.workerId = Random.key64();
		this.producer = new KafkaProducer<>(pProps);
		this.producer.send( ready4Jobs() );
		new Thread( this::handleJobRequests ).start();
	}
	
	void handleJobRequests() {
		cProps.put(ConsumerConfig.GROUP_ID_CONFIG, Random.key64() );	
		try (Consumer<String, String> jobListener = new KafkaConsumer<>(cProps)) {
			jobListener.subscribe(topics(MAPREDUCE));

			this.storage = new RestBlobStorage();
			
			while (true) {
				jobListener.poll(TIMEOUT).forEach(r -> {
					System.out.printf("#### topic = %s, key = %s, value = %s, offset=%s%n", r.topic(), r.key(), r.value(), r.offset());
					Job job = jobs.get( r.value() );
					switch(  MapReduceEvents.valueOf( r.key() )  ) {
						case NEW_JOB: 
							new Job( new JobData(r) ) ; 
							break;
						case FINISH_MAP_PHASE: 
							if( job != null )
								job.finishMapPhase();; 
							break;
						case FINISH_REDUCE_PHASE: 
							if( job != null )
								job.finishReducePhase(); 
							break;
						case END_JOB: 
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
		return Jobs.newJobInstance(storage, jobClassBlob);
	}
	
	class Job extends MapReduceJob {
		final String jobId;
		boolean completed = false; 
		
		AtomicInteger pendingTasks = new AtomicInteger();
		
		Job( JobData data ) {
			super(getJob(data.jobClassBlob), KafkaMapReduceWorker.this.storage, data.inputPrefix, data.outputPrefix, workerId);
			this.jobId = data.jobId;
			jobs.put( this.jobId, this);
			new Thread( this::handleEvents ).start();
		}
		
		public void handleEvents() {				
			cProps.put(ConsumerConfig.GROUP_ID_CONFIG, "task-stealing-group");
			try (Consumer<String, String> consumer = new KafkaConsumer<>(cProps)) {
				producer.send( ready4Tasks(jobId) );
				
				subscribe( consumer, tasksTopic(jobId));
				
				while (! completed ) {
					consumer.poll(TIMEOUT).forEach( r -> {
						System.out.printf("topic = %s, key = %s, value = %s, offset=%s, partition=%s\n", r.topic(), r.key(), r.value(), r.offset(), r.partition());
						switch(  MapReduceEvents.valueOf( r.key() )  ) {
							case DO_MAP_TASK: 
								doMapTask( new TaskData(r));
								break;
							case DO_REDUCE_TASK: 
								doReduceTask( new TaskData(r));					
								break;
							default:
								break;
						}
					});
				}
			}
		}
		
		synchronized void doMapTask( TaskData data ) {			
			pendingTasks.incrementAndGet();
			System.out.println( data.taskId );
			storage.listBlobs( data.inputPrefix).forEach( blob -> {
				storage.readBlob( blob).forEach( System.out::println );
			});
			System.out.println( "---------------------------------");
			
			getMapper().execute(data.inputPrefix);
			producer.send(ackMapTask( jobId, data.taskId, workerId));
			pendingTasks.decrementAndGet();
		}

		synchronized void doReduceTask( TaskData data ) {
			pendingTasks.incrementAndGet();
			getReducer().execute(data.inputPrefix);
			producer.send(ackReduceTask( jobId, data.taskId, workerId));
			pendingTasks.decrementAndGet();
		}

		synchronized void finishMapPhase() {
			while( pendingTasks.get() > 0 )
				Sleep.ms(50);
			
			getMapper().commit();			
			producer.send( ackCommitMapResults( jobId, workerId));
		}

		synchronized void finishReducePhase() {
			while( pendingTasks.get() > 0 )
				Sleep.ms(50);

			getReducer().commit();			
			producer.send(ackCommitReduceResults( jobId, workerId));
			completed = true;
		}
		
		void dispose() {
			jobs.remove( jobId );
		}	
	}
}
