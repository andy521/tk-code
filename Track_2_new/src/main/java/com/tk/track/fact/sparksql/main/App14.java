package com.tk.track.fact.sparksql.main;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by t-chenhao01 on 2016/9/12.
 */
public class App14{
    private static final String CONFIG_PATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_TRACK_CONFIG_PATH_ROOT) + "/appid-type.properties";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Taikang Track Seed Product ETL APP14");
        conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM,
                TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));

        SparkContext sc = new SparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);
        RandomUDF.genrandom(sc, sqlContext);
        App.loadUbaLogEvent(sqlContext, "TMP_UBA_LOG_EVENT", false);
        App.loadSeedProduct(sqlContext,true,false);
        //TODO 测试用
//        String hql = "select * from testhive.uba_log_seedtest";
//
//        DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_UBA_LOG_EVENT");
//
//        SeedIdProductAnalyse spa=new SeedIdProductAnalyse();
//        DataFrame df=spa.getExtPageLoadDF(sqlContext);
//        if(df!=null){
//           DataFrame fdfi= spa.analyseReferId(sqlContext, df);
//            saveAsParquet(fdfi, "/user/tkonline/taikangtrack/data/seed_referId_final");
//        }
//        //TODO

        sqlContext.clearCache();
        sc.stop();
    }
//    public static void saveAsParquet(DataFrame df, String path) {
//        TK_DataFormatConvertUtil.deletePath(path);
//        df.saveAsParquetFile(path);
//    }

}
