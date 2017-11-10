package com.tk.track.fact.sparksql.main;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.util.TK_DataFormatConvertUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by t-chenhao01 on 2016/11/25.
 * 年货节活动线索
 */
public class App28 {
    private static final String CONFIG_PATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_TRACK_CONFIG_PATH_ROOT) + "/appid-type.properties";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TK Track UserBehavior SpingFestivalPurchases Clue ETL APP28");
        conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
        SparkContext sc = new SparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);

        String factUserInfoPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_USERINFO_INPUTPATH);
        App.loadParquet(sqlContext, factUserInfoPath, "FACT_USERINFO", false);

        String appids = TK_CommonConfig.getConfigValue(CONFIG_PATH, "spring_festival_purchases_appid");
        System.out.println("=============appids====================" + appids);
        App.loadSpringFestivalPurchasesClue(sqlContext,appids);

        sqlContext.clearCache();
        sc.stop();
    }
    public static void saveAsParquet(DataFrame df, String path) {
        TK_DataFormatConvertUtil.deletePath(path);
        df.saveAsParquetFile(path);
    }

}
