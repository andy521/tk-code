package com.tk.track.fact.sparksql.main;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.etl.MonthActiveUser;
import com.tk.track.fact.sparksql.hbase.ActiveUser2Hbase;
import com.tk.track.fact.sparksql.udf.RandomUDF;

/**
 * 月活跃度登录过的
 * @author itw_shanll
 *
 */
public class App50 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Taikang Track Month Active ETL APP49");
		conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
		
		SparkContext sc = new SparkContext();
		HiveContext sqlContext = new HiveContext(sc);
		RandomUDF.genrandom(sc, sqlContext);
		String ubaLogEvent = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_UBALOGEVENT_OUTPUTPATH);
		App.loadParquet(sqlContext,ubaLogEvent, "TMP_UBA_LOG_EVENT", false);
		
		String p_memberPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_PMEMBER_PARQUET);
		App.loadParquet(sqlContext, p_memberPath, "TMP_P_MEMBER", false);
		
		MonthActiveUser.loadMonthActiveUser(sqlContext);
		
		
		ActiveUser2Hbase activeUser2Hbase = new ActiveUser2Hbase();
		activeUser2Hbase.loadToHbase(sqlContext);
		
		sqlContext.clearCache();
		sc.stop();
	}
}
