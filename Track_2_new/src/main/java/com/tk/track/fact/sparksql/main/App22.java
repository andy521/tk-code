package com.tk.track.fact.sparksql.main;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;

public class App22 {
	public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Taikang Track EStationWeb ETL APP22 EWEB2HOME");
        conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, 
                TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
        SparkContext sc = new SparkContext(conf);

        HiveContext sqlContext = new HiveContext(sc);
        RandomUDF.genrandom(sc, sqlContext);
        App.loadEStationUserbehaviorData(sqlContext);
		sqlContext.clearCache();
		sc.stop();
	}
}
