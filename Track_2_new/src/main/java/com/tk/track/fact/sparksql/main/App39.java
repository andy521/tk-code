package com.tk.track.fact.sparksql.main;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

public class App39 {

    private static final String CONFIG_PATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_TRACK_CONFIG_PATH_ROOT) + "/appid-type.properties";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Taikang Track 6 month activities ETL APP39");
        conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));

        SparkContext sc = new SparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);
        String factUserInfoPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_USERINFO_INPUTPATH);
        App.loadParquet(sqlContext, factUserInfoPath, "FACT_USERINFO", false);
        String appids = TK_CommonConfig.getConfigValue(CONFIG_PATH, "act.month6.appids");
        App.loadUserBehaviorActivityMonth6(sqlContext, appids);

        sqlContext.clearCache();
        sc.stop();
    }
}
