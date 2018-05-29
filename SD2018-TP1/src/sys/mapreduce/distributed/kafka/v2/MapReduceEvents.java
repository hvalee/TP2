package sys.mapreduce.distributed.kafka.v2;

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
		FINISHED_MAP_PHASE,
		START_REDUCE_PHASE,
		FINISHED_REDUCE_PHASE,
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
	
	public static String jobTopic( String job ) {
		return "mrJob-"+ job;
	}

	// for Workers
	static public ProducerRecord<String, String> newJob(String jobId, String jobClassBlob, String inputPrefix, String outputPrefix ) {
		return new ProducerRecord<>(MAPREDUCE.name(), 0, NEW_JOB.name(), encode(jobId, jobClassBlob, inputPrefix, outputPrefix));
	}

	static public ProducerRecord<String, String> startReducePhase(String jobId) {
		return new ProducerRecord<>(MAPREDUCE.name(), 0, START_REDUCE_PHASE.name(), jobId);
	}

	static public ProducerRecord<String, String> endJob(String jobId) {
		return new ProducerRecord<>(MAPREDUCE.name(), 0, END_JOB.name(), jobId);
	}
	
	// for Master
	static public ProducerRecord<String, String> ready4Jobs(String workerId) {
		return new ProducerRecord<>(MAPREDUCE.name(), 0, WORKER_READY.name(), workerId);
	}

	static public ProducerRecord<String, String> ready4Acks(String jobId) {
		return new ProducerRecord<>(jobTopic(jobId), jobId, "master");
	}

	static public ProducerRecord<String, String> ackJob(String jobId, String workerId) {
		return new ProducerRecord<>(jobTopic(jobId), 0, ACK_JOB.name(), workerId);
	}

	static public ProducerRecord<String, String> ackMapPhase(String jobId, String workerId) {
		return new ProducerRecord<>(jobTopic(jobId), 0, FINISHED_MAP_PHASE.name(), workerId );
	}

	static public ProducerRecord<String, String> ackReducePhase(String jobId, String workerId ) {
		return new ProducerRecord<>(jobTopic(jobId), 0, FINISHED_REDUCE_PHASE.name(), workerId);
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
}
