package com.tk.track.fact.sparksql.main;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;

public class App9 {
	public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Taikang Track FACT_USERBEHAVIOR_INDEX ETL APP9");
        conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, 
                TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
        SparkContext sc = new SparkContext(conf);

        HiveContext sqlContext = new HiveContext(sc);
        RandomUDF.genrandom(sc, sqlContext);        
        App.loadUbaLogEvent(sqlContext,"TMP_UBA_LOG_EVENT",false);
        App.loadFactUserBehaviorIndex(sqlContext,true,false);
        
		sqlContext.clearCache();
		sc.stop();
	}
}
