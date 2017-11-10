package com.taikang.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Demo01_Producer {
	
	public static void main(String[] args) {
		loadProperties();
		
		producer();
	}
	
	
	private static void producer(){
		KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
		for (int i = 0; i < 100; i++) {
			producer.send(new ProducerRecord<String, String>("test", "key-" + i, "value-" + i));
		}
		producer.close();
	}
	
	private static Properties prop = new Properties();
	private static void loadProperties(){
		prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "tkol-node03:9092");
		prop.put(ProducerConfig.ACKS_CONFIG, "all");
		prop.put(ProducerConfig.RETRIES_CONFIG, 0);
		prop.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 16);
		prop.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		prop.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1024 * 1024 * 32);
		prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		prop.setProperty("kafka.topics", "test");
		prop.setProperty("kafka.consumer.lens", "10");
		
	}
}
