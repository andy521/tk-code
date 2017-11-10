package com.tk.track.fact.sparksql.main;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;

public class App11 {

	// 外部配置文件
	private static final String CONFIG_PATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_TRACK_CONFIG_PATH_ROOT) + "/appid-type.properties";

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Taikang Track UserBehaviorYlsq ETL APP11");
		conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
		SparkContext sc = new SparkContext(conf);

		HiveContext sqlContext = new HiveContext(sc);
		RandomUDF.genrandom(sc, sqlContext);
		RandomUDF.gendecode(sc, sqlContext);
		String staticsEventPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH);
		App.loadParquet(sqlContext, staticsEventPath, "F_STATISTICS_EVENT", false);
		String p_memberPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_PMEMBER_PARQUET);
		App.loadParquet(sqlContext, p_memberPath, "P_MEMBER", false);
		String appids = TK_CommonConfig.getConfigValue(CONFIG_PATH, "ylsq.appids");

		RandomUDF.mseconds2hms(sc, sqlContext);

		App.loadFactUserBehaviorYlsq(sqlContext, appids);
		sqlContext.clearCache();
		sc.stop();
	}

}
