package com.tk.track.fact.sparksql.main;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;

public class App30 {
	private static final String CONFIG_PATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_TRACK_CONFIG_PATH_ROOT) + "/appid-type.properties";

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Taikang Track CLUE_SYSTEM_USE_CONDITION ETL APP30");
	    conf.set("hbase.zookeeper.quorum", 
	      TK_CommonConfig.getValue("hbase.zookeeper.quorum"));

	    SparkContext sc = new SparkContext(conf);
	    HiveContext sqlContext = new HiveContext(sc);
	    RandomUDF.genrandom(sc, sqlContext);

        String appids = TK_CommonConfig.getConfigValue(CONFIG_PATH, "tele.system.use.condition");
        App.loadTeleSystemUseCondition(sqlContext, appids);
	   
        sqlContext.clearCache();
	    sc.stop();
	}
}
