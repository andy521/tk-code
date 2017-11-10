package com.tk.track.fact.sparksql.main;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.etl.SeedRecomender;
import com.tk.track.fact.sparksql.hbase.RecomenderShare2Hbase;
import com.tk.track.fact.sparksql.udf.RandomUDF;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by itw_chenhao01 on 2017/8/15.
 */
public class App47 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Taikang Track Seed Recomender  ETL APP47");
        conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM,
                TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));

        SparkContext sc = new SparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);
        RandomUDF.genrandom(sc, sqlContext);
        App.loadUbaLogEvent(sqlContext, "TMP_UBA_LOG_EVENT", false);

        SeedRecomender.loadSeedProduct(sqlContext,true,false);

        RecomenderShare2Hbase share2Hbase=new RecomenderShare2Hbase();
        share2Hbase.loadToHbase(sqlContext);
        share2Hbase.inserConfigTable();

        sqlContext.clearCache();
        sc.stop();
    }
}
