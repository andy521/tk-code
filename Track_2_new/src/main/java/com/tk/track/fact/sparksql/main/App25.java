package com.tk.track.fact.sparksql.main;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;

public class App25 {

	// 外部配置文件 理赔流程appid
	private static final String CONFIG_PATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_TRACK_CONFIG_PATH_ROOT) + "/appid-type.properties";
	
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Taikang Track UserBehavior WechatHealthTest ETL APP25");
		conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
		SparkContext sc = new SparkContext(conf);

		HiveContext sqlContext = new HiveContext(sc);
		
		String staticsEventPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH);
		App.loadParquet(sqlContext, staticsEventPath, "F_STATISTICS_EVENT", false);

        
        String factUserInfoPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_USERINFO_INPUTPATH);
        App.loadParquet(sqlContext, factUserInfoPath, "FACT_USERINFO", false);
        
        String factTestScorePath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTHEALTHTESTSCORE_INPUTPATH);
        App.loadParquet(sqlContext, factTestScorePath, "FACT_HEALTHTESTSCORE", false);
        
		String appids = TK_CommonConfig.getConfigValue(CONFIG_PATH, "health_test.appids");
	    System.out.println("=============health_test.appids====================" + appids);

		App.loadFactUserBehaviorWechatHealthTest(sqlContext, appids);
		sqlContext.clearCache();
		sc.stop();
	}
	

}
