package com.tk.track.fact.sparksql.main;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;

/**
 * 网电结合项目-app活跃用户线索导入
 * @author itw_shanll
 *
 */
public class App46 {

	private static final String CONFIG_PATH= TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_TRACK_CONFIG_PATH_ROOT)+ "/appid-type.properties";
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Taikang Track UBA_LOG_EVENT ETL APP46");
		conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
		
		SparkContext sc = new SparkContext(conf);
		HiveContext sqlContext = new HiveContext(sc);
		RandomUDF.getUUID(sc, sqlContext);
		String ubaLogEvent = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_UBALOGEVENT_OUTPUTPATH);
		App.loadParquet(sqlContext,ubaLogEvent, "UBA_LOG_EVENT", false);
		
		String factUserInfoPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_USERINFO_INPUTPATH);
		App.loadParquet(sqlContext, factUserInfoPath, "FACT_USERINFO", false);
		
		String p_memberPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_PMEMBER_PARQUET);
		App.loadParquet(sqlContext, p_memberPath, "P_MEMBER", false);
		
//		String appids = TK_CommonConfig.getConfigValue(CONFIG_PATH, "wechat.hot");
		App.loadAppActiveUser(sqlContext);
		sqlContext.clearCache();
		sc.stop();
	}

}
