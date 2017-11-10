package com.tk.track.fact.sparksql.main;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;

public class App999 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Taikang Track CLUE COMMON ETL APP999");
        conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));

        SparkContext sc = new SparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);
        String factUserInfoPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_USERINFO_INPUTPATH);
        App.loadParquet(sqlContext, factUserInfoPath, "FACT_USERINFO", false);
        String factStatisticEventPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH);
        App.loadParquet(sqlContext, factStatisticEventPath, "F_STATISTIC_EVENT", false);
        App.loadCommonUserBehaviorClue(sqlContext);

        sqlContext.clearCache();
        sc.stop();
    }
}
