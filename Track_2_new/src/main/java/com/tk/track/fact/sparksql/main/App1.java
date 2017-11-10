package com.tk.track.fact.sparksql.main;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.desttable.FactSrcUserEvent;
import com.tk.track.fact.sparksql.etl.SrcUserEvent;
import com.tk.track.fact.sparksql.udf.RandomUDF;

public class App1 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Taikang Track UAB_LOG_EVENT ETL APP1");
        conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, 
                TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
        SparkContext sc = new SparkContext(conf);

        HiveContext sqlContext = new HiveContext(sc);
        RandomUDF.genrandom(sc, sqlContext);
        
        //1. read log file for raw data
        // Table1: uba_log_event 
        // load from hbase::src_user_event to FACR_LOG_EVENT
        App.loadRawData(sc, sqlContext, true);

        //2. Table2: uba_user_event
        //load from hbase::src_user_event(Table1) to FACR_USER_EVENT
        App.loadUbaUserEvent(sqlContext);

        //3. Table3: uba_statistics_page 
        //load from hbase::src_user_event(Table1) to FactStatisticsPage
        App.loadUbaStatisticsPage(sqlContext);
        
        ////////////////////////////////////////////////////////////////
        sqlContext.clearCache();
        sc.stop();
    }

}
