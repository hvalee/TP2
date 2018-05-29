package sys.mapreduce.distributed.kafka.v1;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public enum MapReduceEvents {
	MAPREDUCE,
	NEW_JOB,
	ACK_JOB,
	END_JOB,
	DO_MAP_TASK,
	ACK_MAP_TASK,
	DO_REDUCE_TASK,
	ACK_REDUCE_TASK,
	FINISH_MAP_PHASE,
	ACK_FINISH_MAP_PHASE,
	FINISH_REDUCE_PHASE,
	ACK_FINISH_REDUCE_PHASE,
	WORKER_READY;
		
	private static final String DELIMITER = "@";
	
	public static Collection<String> topics( MapReduceEvents ... topics ) {
		return Arrays.asList( topics ).stream().map( ev -> ev.name() ).collect( Collectors.toList() );
	}
		
	private static String encode(String ... values ) {
		return Arrays.asList( values ).stream().collect( Collectors.joining(DELIMITER));
	}
	
	private static String[] decode(String values ) {
		return values.split(DELIMITER);
	}
	
	public static String tasksTopic( String job ) {
		return "mr-tasks-"+ job;
	}

	public static String acksTopic( String job ) {
		return "mr-acks-"+ job;
	}

	static public ProducerRecord<String, String> newJob(String jobId, String jobClassBlob, String inputPrefix, String outputPrefix ) {
		return new ProducerRecord<>(MAPREDUCE.name(), NEW_JOB.name(), encode(jobId, jobClassBlob, outputPrefix));
	}

	static public ProducerRecord<String, String> endJob(String jobId) {
		return new ProducerRecord<>(MAPREDUCE.name(), END_JOB.name(), jobId);
	}
	
	static public ProducerRecord<String, String> finishMapPhase(String jobId) {
		return new ProducerRecord<>(MAPREDUCE.name(), FINISH_MAP_PHASE.name(), jobId);
	}
	
	static public ProducerRecord<String, String> finishReducePhase(String jobId) {
		return new ProducerRecord<>(MAPREDUCE.name(), FINISH_REDUCE_PHASE.name(), jobId);
	}

	static public ProducerRecord<String, String> ready4Jobs() {
		return new ProducerRecord<>(MAPREDUCE.name(), "", "");
	}

	static public ProducerRecord<String, String> ready4Tasks(String jobId) {
		return new ProducerRecord<>(tasksTopic(jobId), jobId, "slave");
	}
	
	static public ProducerRecord<String, String> ready4Acks(String jobId) {
		return new ProducerRecord<>(acksTopic(jobId), jobId, "master");
	}
	
	static public ProducerRecord<String, String> mapTask(String jobId, String taskId, String inputPrefix) {
		return new ProducerRecord<>(tasksTopic(jobId), DO_MAP_TASK.name(), encode(taskId, inputPrefix));
	}
	
	static public ProducerRecord<String, String> ackMapTask(String jobId, String taskId, String workerId) {
		return new ProducerRecord<>(acksTopic(jobId), ACK_MAP_TASK.name(), encode(taskId, workerId));
	}

	static public ProducerRecord<String, String> reduceTask(String jobId, String taskId, String inputPrefix) {
		return new ProducerRecord<>(tasksTopic(jobId), DO_REDUCE_TASK.name(), encode(taskId, inputPrefix));
	}
		
	static public ProducerRecord<String, String> ackReduceTask(String jobId, String taskId, String workerId) {
		return new ProducerRecord<>(acksTopic(jobId), ACK_REDUCE_TASK.name(), encode(taskId, workerId));
	}
	
	static public ProducerRecord<String, String> ackCommitMapResults(String jobId, String workerId) {
		return new ProducerRecord<>(acksTopic(jobId), ACK_FINISH_MAP_PHASE.name(), encode(workerId, workerId));
	}
	
	static public ProducerRecord<String, String> ackCommitReduceResults(String jobId, String workerId) {
		return new ProducerRecord<>(acksTopic(jobId), ACK_FINISH_REDUCE_PHASE.name(), encode(workerId, workerId));
	}

	
	static class AckEvent {
		public final String jobId, taskId, workerId;
		
		AckEvent( ConsumerRecord<String, String> r ) {
			String[] tokens = decode( r.value() );
			this.jobId = r.topic();
			this.taskId = tokens[0];
			this.workerId = tokens[1];
		}
	} 

	static class JobData {
		public final String jobId, jobClassBlob, inputPrefix, outputPrefix;
		
		JobData( ConsumerRecord<String, String> r ) {
			String[] tokens = decode( r.value() );
			this.jobId = tokens[0];
			this.jobClassBlob = tokens[1];
			this.inputPrefix = tokens[2];
			this.outputPrefix = tokens[3];
		}
	} 
	
	static class TaskData {
		public final String jobId, taskId, inputPrefix;
		
		TaskData( ConsumerRecord<String, String> r ) {
			this.jobId = r.topic();
			String[] tokens = r.key().split(DELIMITER);
			this.taskId = tokens[1];
			this.inputPrefix = r.value();
		}
	}
}
