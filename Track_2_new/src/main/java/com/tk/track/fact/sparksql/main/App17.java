package com.tk.track.fact.sparksql.main;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;

public class App17 {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Taikang Track UserBehaviorTylp ETL APP17");
        conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, 
                TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
        SparkContext sc = new SparkContext(conf);

        HiveContext sqlContext = new HiveContext(sc);
        String pLifeinsurePath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_PLIFEINSURE_PARQUET);	        //加载P_LIFEINSURE
        App.loadParquet(sqlContext, pLifeinsurePath, "P_LIFEINSURE", false);
        String staticsEventPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH);
        App.loadParquet(sqlContext, staticsEventPath, "FACT_STATISTICS_EVENT", false);
        String factUserInfoPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_USERINFO_INPUTPATH);
        App.loadParquet(sqlContext, factUserInfoPath, "FACT_USERINFO", false);
        App.loadUserBehaviorTylp(sqlContext);
		sqlContext.clearCache();
		sc.stop();
		
		
	}
	
}
