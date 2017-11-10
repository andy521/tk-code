/**
 * @Title: App2.java
 * @Package: com.tk.track.fact.sparksql.main
 * @Description: TODO
 * @author zhang.shy
 * @Data 2016年5月16日
 * @version 1.0
 */
package com.tk.track.fact.sparksql.main;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;

/**
 * @ClassName: App2
 * @Description: TODO
 * @author zhang.shy
 * @date   2016年5月16日 
 */
public class App2 {
    
    
    public static void main(String[] args) {
        
        SparkConf conf = new SparkConf().setAppName("Taikang Track FACT_STATISTICS_EVENT ETL APP2");
        conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, 
                TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
        SparkContext sc = new SparkContext(conf);

        HiveContext sqlContext = new HiveContext(sc);
        RandomUDF.genrandom(sc, sqlContext);
        RandomUDF.gendecode(sc, sqlContext);
        
        //1. read log file for raw data
        // Table1: uba_log_event 
        // load from hbase::src_user_event to FACR_LOG_EVENT
        App.loadRawData(sc, sqlContext, false);
        App.loadUbaLogEvent(sqlContext, "TMP_UBA_LOG_EVENT", false);
        
        //2. Table2: f_terminal_map 
        //load from hbase::src_user_event(Table1) to F_TERMINAL_MAP
        App.loadFTerminalMap(sqlContext);
        String factUserInfoPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_USERINFO_INPUTPATH);
        App.loadParquet(sqlContext, factUserInfoPath, "FACT_USERINFO", false);
        App.loadFTerminalMapApp(sqlContext,true,false);
        
        //3. Table3: f_statistics_event
        //load from hbase::src_user_event(Table1) to F_STATISTICS_EVENT
        App.loadFStatisticsEvent(sqlContext);
        App.DeleteCachetable();
        sqlContext.clearCache();
        sc.stop();
    }

}
