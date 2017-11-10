package com.tk.track.fact.sparksql.main;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;

public class App18 {


	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Taikang Track UserBehaviorWechatMy ETL APP18");
		conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
		SparkContext sc = new SparkContext(conf);

		HiveContext sqlContext = new HiveContext(sc);
		
		String staticsEventPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH);
		App.loadParquet(sqlContext, staticsEventPath, "F_STATISTICS_EVENT", false);
		String factUserInfoPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_USERINFO_INPUTPATH);
        App.loadParquet(sqlContext, factUserInfoPath, "FACT_USERINFO", false);
        
        String fnetpolicysummary = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FNETPOLICYSUMMARY_OUTPUTPATH);
        App.loadParquet(sqlContext, fnetpolicysummary, "F_NET_POLICYSUMMARY", false);

		App.loadFactUserBehaviorWechatMy(sqlContext);
		
		
		sqlContext.clearCache();
		sc.stop();
	}

}
