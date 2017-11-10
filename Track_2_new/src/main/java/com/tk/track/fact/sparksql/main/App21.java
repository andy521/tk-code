package com.tk.track.fact.sparksql.main;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;

public class App21 {
	
	private static final String CONFIG_PATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_TRACK_CONFIG_PATH_ROOT) + "/appid-type.properties";

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Taikang Track FACT_USER_BEHAVIOR_CLUE_HEALTH_SERVICE ETL APP 21");
	    conf.set("hbase.zookeeper.quorum", 
	      TK_CommonConfig.getValue("hbase.zookeeper.quorum"));

	    SparkContext sc = new SparkContext(conf);
	    HiveContext sqlContext = new HiveContext(sc);
	    RandomUDF.genrandom(sc, sqlContext);

	    String factUserInfoPath = TK_CommonConfig.getValue("taikang.hdfs.userinfo.input");
	    App.loadParquet(sqlContext, factUserInfoPath, "FACT_USERINFO", false);
	    String appids = TK_CommonConfig.getConfigValue(CONFIG_PATH, "health_service");
	    System.out.println("=============appids====================" + appids);
	    App.loadFactUserBehaviorClueHealthService(sqlContext, appids);

	    sqlContext.clearCache();
	    sc.stop();
	}
	
}
