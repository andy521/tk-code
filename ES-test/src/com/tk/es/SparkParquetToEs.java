package com.tk.es;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.spark.rdd.EsSpark;

public class SparkParquetToEs {
	
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("SparkParquetToEs");
		conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "false");
		conf.set(ConfigurationOptions.ES_NODES, "10.130.159.36");
		conf.set(ConfigurationOptions.ES_PORT, "9200");

		SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
		SQLContext sqlContext = sparkSession.sqlContext();
		Dataset dataset = sqlContext.read().parquet(args[0]);
		EsSpark.saveJsonToEs(dataset.rdd(),"index/test");
		
	}

}
