package com.tk.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import common.App;
import common.UserLogInfo;
import scala.Tuple2;

public class Start {
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		
		Properties prop = new Properties();
		prop.setProperty(KafKaPropertiesOptions.ZOOKEEPER_NODES, KafKaPropertiesOptions.ZOOKEEPER_NODES_DEFAULT);
		prop.setProperty(KafKaPropertiesOptions.KAFKA_TOPICS, KafKaPropertiesOptions.KAFKA_TOPICS_DEFAULT);
		prop.setProperty(KafKaPropertiesOptions.KAFKA_GROUP, KafKaPropertiesOptions.KAFKA_GROUP_DEFAULT);
		prop.setProperty(KafKaPropertiesOptions.SPARK_NUM_THREADS, KafKaPropertiesOptions.SPARK_NUM_THREADS_DEFAULT);
		prop.setProperty(KafKaPropertiesOptions.SPARK_MASTER, KafKaPropertiesOptions.SPARK_MASTER_DEFAULT);
		prop.setProperty(KafKaPropertiesOptions.SPARK_APP_NAME, KafKaPropertiesOptions.SPARK_APP_NAME_DEFAULT);
		prop.setProperty(KafKaPropertiesOptions.SPARK_LOG_LEVEL, KafKaPropertiesOptions.SPARK_LOG_LEVEL_DEFAULT);
		prop.setProperty(KafKaPropertiesOptions.HBASE_TABLE_NAME, KafKaPropertiesOptions.HBASE_TABLE_NAME_DEFAULT);
		prop.setProperty(KafKaPropertiesOptions.HBASE_HAS2DEL, KafKaPropertiesOptions.HBASE_HAS2DEL_DEFAULT);
		
		loadProp(prop, args);
		
		Demo01 d = new Demo01();
		d.init(prop.getProperty(KafKaPropertiesOptions.HBASE_TABLE_NAME), prop.getProperty(KafKaPropertiesOptions.HBASE_HAS2DEL));
		
		String zkQuorum = prop.getProperty(KafKaPropertiesOptions.ZOOKEEPER_NODES);
		String topics = prop.getProperty(KafKaPropertiesOptions.KAFKA_TOPICS);
		String group = prop.getProperty(KafKaPropertiesOptions.KAFKA_GROUP);
		int numThreads = 1;
		try {
			if(prop.contains(KafKaPropertiesOptions.SPARK_NUM_THREADS)){
				numThreads = Integer.parseInt(prop.getProperty(KafKaPropertiesOptions.SPARK_NUM_THREADS));
			}
		} catch (Exception e) {}
		
		SparkConf sparkConf = new SparkConf()
								.setMaster(prop.getProperty(KafKaPropertiesOptions.SPARK_MASTER))
								.setAppName(prop.getProperty(KafKaPropertiesOptions.SPARK_APP_NAME));
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.setLogLevel(prop.getProperty(KafKaPropertiesOptions.SPARK_LOG_LEVEL));
		
		JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(Integer.parseInt(prop.getProperty(KafKaPropertiesOptions.SPARK_STREAMING_DURATIONS))));
		
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		for (String topic : topics.split(",")) {
			topicMap.put(topic, numThreads);
		}
		
		JavaPairDStream<String, String> streamRDD = KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);
		
		streamRDD.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>(){
			@Override
			public void call(JavaPairRDD<String, String> rdd) throws Exception {
				List<Tuple2<String, String>> list = rdd.collect();
				for (Tuple2<String, String> tuple : list) {
					String val = tuple._2;
					try {
						List<UserLogInfo> userList = App.analysisContext(val);
						for (UserLogInfo userLogInfo : userList) {
							Map<String, Object> data = null;
							if(userLogInfo == null){
								data = new HashMap<>();
								data.put("_key", tuple._1);
								data.put("_val", tuple._2);
								d.insertData(UUID.randomUUID().toString(), data);
							}else{
								data = d.exchangeData(userLogInfo);
								d.insertData(userLogInfo.getROWKEY(), data);
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
						System.out.println(val);
					}
					
				}
			}
		});
		
		jssc.start();
		
		jssc.awaitTermination();
	}

	private static void loadProp(Properties prop, String[] args) {
		if(args.length == 0 || !new File(args[0]).exists()){
			System.out.println("配置文件不存在！！！");
			System.exit(-1);
		}
		
		Properties p = new Properties();
		try (FileInputStream fis = new FileInputStream(args[0])) {
			p.load(fis);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("加载配置文件失败！！！");
			System.exit(-1);
		}
		
		p.keySet().forEach(k -> {
			String key = (String) k;
			prop.setProperty(key, p.getProperty(key));
		});

	}
	
}
