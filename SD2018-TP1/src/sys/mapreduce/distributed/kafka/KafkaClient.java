package sys.mapreduce.distributed.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import utils.Random;

public class KafkaClient {

	final protected Properties pProps, cProps;

	protected KafkaClient() {
		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "OFF");

		pProps = new Properties();
		pProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092,kafka2:9092,kafka3:9092");
		pProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		pProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
				
		cProps = new Properties();
		cProps.put(ConsumerConfig.GROUP_ID_CONFIG, Random.key64() );
		cProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		cProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092,kafka2:9092,kafka3:9092");
		cProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		cProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	}
	
	
	// Assuming the consumers of the topic do not share the group.id, this will be a broadcast(), ie., all will get the event.
	protected void broadcast( Producer<String,String> producer, ProducerRecord<String,String> r) {
		producer.send( r) ;
	}
	
	// Assuming the consumers of the topic share the same group.id, this will be a anycast(), ie., only one will get the event.
	protected void anycast( Producer<String,String> producer, ProducerRecord<String,String> r) {
		producer.send( r) ;
	}
	
	protected void subscribe( Consumer<String, String> consumer, String ... topics ) {
		consumer.subscribe( Arrays.asList( topics ));
	}
}
