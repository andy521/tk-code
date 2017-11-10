package com.tk.track.fact.sparksql.main;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;

public class App20 {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Taikang Track UserBehavior_browse_website ETL APP20");
        conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, 
                TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
        SparkContext sc = new SparkContext(conf);

        HiveContext sqlContext = new HiveContext(sc);
        String staticsEventPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH);
        App.loadParquet(sqlContext, staticsEventPath, "FACT_STATISTICS_EVENT", false);
        String factUserInfoPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_USERINFO_INPUTPATH);
        App.loadParquet(sqlContext, factUserInfoPath, "FACT_USERINFO", false);
        App.loadUserBehaviorBrowseWebsite(sqlContext);
		sqlContext.clearCache();
		sc.stop();
		
		
	}
}
