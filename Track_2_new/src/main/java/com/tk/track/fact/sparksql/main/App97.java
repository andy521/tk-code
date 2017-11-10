package com.tk.track.fact.sparksql.main;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.etl.RenewalMessage;
import com.tk.track.util.TK_DataFormatConvertUtil;
import com.tk.track.util.TimeUtil;

import scala.collection.generic.BitOperations.Int;

public class App97 {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Taikang RENEWLMASSAGE ETL APP_97");
		SparkContext sc = new SparkContext(conf);
		HiveContext sqlContext = new HiveContext(sc);
		sqlContext.load(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACT_USERINFO)).registerTempTable("FACT_USERINFO");
		RenewalMessage rm =  new RenewalMessage();
		DataFrame df = rm.getDataFrame(sqlContext);
		Date date = new Date();
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		String formatdate = format.format(date);
		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RENEWAL_PARQUET)+formatdate;
		if(TK_DataFormatConvertUtil.isExistsPath(path)){
			TK_DataFormatConvertUtil.deletePath(path);
		}
		df.save(path);
		//存入hbase
		sqlContext.load(path).registerTempTable("RENEWALPOLICY2");
		RenewalMessage.loadToHbase(sqlContext,TimeUtil.getNowStr("yyyy-MM-dd"));
		sqlContext.clearCache();
		sc.stop();
	}
}
