package com.tk.track.fact.sparksql.main;

import java.text.SimpleDateFormat;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;

public class App3 {

	public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Taikang Track FACT_USER_BEHAVIOR_TELE ETL APP3");
        conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, 
                TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
        SparkContext sc = new SparkContext(conf);
        
        HiveContext sqlContext = new HiveContext(sc);
        RandomUDF.genrandom(sc, sqlContext);        
        String factUserInfoPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_USERINFO_INPUTPATH);
        App.loadParquet(sqlContext, factUserInfoPath, "FACT_USERINFO", false);
        String teleAllPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORTELE_OUTPUTPATH);
        App.loadParquet(sqlContext, teleAllPath, "Tmp_teleAll", false);
        App.loadFactUserBehaviorTele(sqlContext);
		sqlContext.clearCache();
		sc.stop();
	}
	
}
