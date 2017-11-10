package com.tk.kafka;

public interface KafKaPropertiesOptions {
	
	String ZOOKEEPER_NODES = "zookeeper.nodes";
	String ZOOKEEPER_NODES_DEFAULT = "localhost:2181";

	String KAFKA_TOPICS = "kafka.topics";
	String KAFKA_TOPICS_DEFAULT = "test";
	
	String KAFKA_GROUP = "kafka.group";
	String KAFKA_GROUP_DEFAULT = "test";
	
	String SPARK_NUM_THREADS = "spark.num.threads";
	String SPARK_NUM_THREADS_DEFAULT = "1";
	
	String SPARK_MASTER = "spark.master";
	String SPARK_MASTER_DEFAULT = "yarn-client";
	
	String SPARK_APP_NAME = "spark.app.name";
	String SPARK_APP_NAME_DEFAULT = "test";
	
	String SPARK_LOG_LEVEL = "spark.log.level";
	String SPARK_LOG_LEVEL_DEFAULT = "WARN";
	
	String SPARK_STREAMING_DURATIONS = "spark.streaming.durations";
	String SPARK_STREAMING_DURATIONS_DEFAULT = "10";
	
	String HBASE_TABLE_NAME = "hbase.table.name";
	String HBASE_TABLE_NAME_DEFAULT = "test111000999";
	
	String HBASE_HAS2DEL = "hbase.has2del";
	String HBASE_HAS2DEL_DEFAULT = "false";
	
	
	
}
