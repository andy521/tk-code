package com.taikang.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Demo01_Consumer {
	
	public static void main(String[] args) {
		loadProperties();
		
		consumer();
	}
	
	
	@SuppressWarnings("resource")
	private static void consumer(){
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
		
		String topics = prop.remove("kafka.topics") + "";
//		int lens = Integer.parseInt(prop.remove("kafka.consumer.lens") + "");
		
		consumer.subscribe(Arrays.asList(topics));
		
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
			
			if(records == null) continue;
			
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
			}
		}
	}
	
	private static final long POLL_TIMEOUT = 100L;
	private static Properties prop = new Properties();
	private static void loadProperties(){
		prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "tkol-node03:9092");
		prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
		prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		prop.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		prop.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		
		prop.setProperty("kafka.topics", "test");
		prop.setProperty("kafka.consumer.lens", "10");
		
	}
}
