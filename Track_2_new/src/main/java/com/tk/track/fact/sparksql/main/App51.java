package com.tk.track.fact.sparksql.main;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;

/**
 * 财险一年期续期线索
 * @author itw_shanll
 *
 */
public class App51 {

//	private static final String CONFIG_PATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_TRACK_CONFIG_PATH_ROOT)+"/appid-type.properties";
	
	public static void main(String[] args) {
//		SparkConf conf = new SparkConf().setAppName("Taikang track INSURANCE_ONEYEAR_RENEWAL ETL APP51");
//		conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
//		
//		SparkContext sc = new SparkContext(conf);
//		HiveContext sqlContext = new HiveContext(sc);
//		RandomUDF.getUUID(sc, sqlContext);
//		
//		Date date = new Date();
//		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
//		String formatdate = format.format(date);
//		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RENEWAL_PARQUET)+formatdate;
//		App.loadParquet(sqlContext, path, "TMP_RENEWAL", false);
//		
//		String p_memberPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_PMEMBER_PARQUET);
//		App.loadParquet(sqlContext, p_memberPath, "P_MEMBER", false);
//		
//		App.oneyearRenewal(sqlContext);
//		sqlContext.clearCache();
//		sc.stop();
	}
}
