package com.tk.track.fact.sparksql.main;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;

public class App43 {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Taikang Track CLUE_SYSTEM_USE_CONDITION ETL APP43");
	    conf.set("hbase.zookeeper.quorum", 
	      TK_CommonConfig.getValue("hbase.zookeeper.quorum"));

	    SparkContext sc = new SparkContext(conf);
	    HiveContext sqlContext = new HiveContext(sc);
	    RandomUDF.genrandom(sc, sqlContext);
        App.loadTeleShareSummary(sqlContext);
	   
        sqlContext.clearCache();
	    sc.stop();
	}
}
