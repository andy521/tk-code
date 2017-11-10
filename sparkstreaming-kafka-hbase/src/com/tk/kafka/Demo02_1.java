package com.tk.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class Demo02_1 {
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		
//		Demo01 d = new Demo01();
		Demo01_spark1 d = new Demo01_spark1();
		d.init(args[0]);
		
		String zkQuorum = "10.130.159.11:2181,10.130.159.12:2181,10.130.159.13:2181";
		String topics = "test";
		String group = "test";
		int numThreads = 1;
		
		SparkConf sparkConf = new SparkConf().setMaster("yarn-client").setAppName("kafka2hbase");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.setLogLevel("WARN");
		JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(10));
		
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		for (String topic : topics.split(",")) {
			topicMap.put(topic, numThreads);
		}
		
		JavaPairDStream<String, String> streamRDD = 
				KafkaUtils.createStream(jssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER());
		
		streamRDD.map(new Function<Tuple2<String,String>, String>() {
			@Override
			public String call(Tuple2<String, String> tuple) throws Exception {
				return tuple._1 + ",,," + tuple._2;
			}
		}).print();
		
		jssc.start();
		
		jssc.awaitTermination();
		
		/*streamRDD.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
				return tuple;
			}
		}).reduceByKey(new Function2<String, String, String>() {
			@Override
			public String call(String v1, String v2) throws Exception {
				return null;
			}
		});*/
	}
	
}
