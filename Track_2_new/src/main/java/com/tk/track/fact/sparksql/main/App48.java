package com.tk.track.fact.sparksql.main;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;

/**
 * 京鄂鲁老客户
 * @author itw_shanll
 *
 */
public class App48 {

	private static final String CONFIG_PATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_TRACK_CONFIG_PATH_ROOT)+"/appid-type.properties";
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Taikang Track JEL_OLD_COUSTOMER ETL APP48");
		conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
		
		SparkContext sc = new SparkContext(conf);
		HiveContext sqlContext = new HiveContext(sc);
		RandomUDF.getUUID(sc, sqlContext);
		
		String p_lifeinsure = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_PLIFEINSURE_PARQUET);
		App.loadParquet(sqlContext,p_lifeinsure,"P_LIFEINSURE",false);
		
		String p_memberPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_PMEMBER_PARQUET);
		App.loadParquet(sqlContext, p_memberPath, "P_MEMBER", false);
		
		
		App.loadOldCoustomer(sqlContext);
		sqlContext.clearCache();
		sc.stop();
	}
}
