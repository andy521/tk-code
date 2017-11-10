package com.tk.track.fact.sparksql.main;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;

public class App10 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Taikang Track UserBehaviorTeleApp ETL APP10");
        conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, 
                TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
        SparkContext sc = new SparkContext(conf);

        HiveContext sqlContext = new HiveContext(sc);
        RandomUDF.genrandom(sc, sqlContext);        
        RandomUDF.gendecode(sc, sqlContext); 
        String staticsEventPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH);
        App.loadParquet(sqlContext, staticsEventPath, "F_STATISTICS_EVENT", false);
        String factUserInfoPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_USERINFO_INPUTPATH);
        App.loadParquet(sqlContext, factUserInfoPath, "FACT_USERINFO", false);
        String validPolicyPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTNETPOLICYINFORESULT_OUTPUTPATH) + "-" + App.GetSysDate(-1);
        App.loadParquet(sqlContext, validPolicyPath, "FACT_PolicyInfo", false);
        App.loadFactUserBehaviorTeleApp(sqlContext);
		sqlContext.clearCache();
		sc.stop();
	}

}
