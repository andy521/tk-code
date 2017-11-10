package com.tk.track.etl.fact.hive;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.desttable.FactSrcUserEvent;
import com.tk.track.fact.sparksql.desttable.FactStatisticsEvent;
import com.tk.track.fact.sparksql.desttable.FactStatisticsPage;
import com.tk.track.fact.sparksql.desttable.FactTerminalMap;
import com.tk.track.fact.sparksql.desttable.FactUserBehaviorTele;
import com.tk.track.fact.sparksql.desttable.FactUserEvent;
import com.tk.track.fact.sparksql.etl.SrcUserEvent;
import com.tk.track.fact.sparksql.etl.StatisticsEvent;
import com.tk.track.fact.sparksql.etl.StatisticsPage;
import com.tk.track.fact.sparksql.etl.TerminalMap;
import com.tk.track.fact.sparksql.etl.UserBehaviorTele;
import com.tk.track.fact.sparksql.etl.UserEvent;
import com.tk.track.fact.sparksql.udf.RandomUDF;
import com.tk.track.fact.sparksql.util.HBaseUtil;

/*
 * spark-submit --class com.tk.track.etl.fact.hive.SimpleDemo --master yarn /home/renxiaodong/spark/sample/fact.jar
 */
public class SimpleDemo {

//  private static final String FILENAME_READ = "/user/tkonline/taikangscore/temp/logResult";
    
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Taikang Track");
        conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, 
                TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
        SparkContext sc = new SparkContext(conf);

        HiveContext sqlContext = new HiveContext(sc);
        RandomUDF.genrandom(sc, sqlContext);
        
        //1. read log file
        String FILENAME_READ = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_TRACKLOG_INPUTPATH);
        SrcUserEvent mSrcUserEvent = new SrcUserEvent();
        JavaRDD<String> lines = sc.textFile(FILENAME_READ, 1).toJavaRDD();
        //2. filter the raw data
        JavaRDD<FactSrcUserEvent> logRDD = mSrcUserEvent.getJavaRDD(lines);
        
        //3. Table1: uba_log_event 
        //load from hbase::src_user_event to FACR_LOG_EVENT
        loadUbaLogEvent(sqlContext, logRDD);
        
        //registry
        sqlContext.createDataFrame(logRDD, FactSrcUserEvent.class).registerTempTable("Temp_SrcUserEvent");

        //4. Table2: uba_user_event
        //load from hbase::src_user_event(Table1) to FACR_USER_EVENT
        loadUbaUserEvent(sqlContext);

        //5. Table3: uba_statistics_page 
        //load from hbase::src_user_event(Table1) to FactStatisticsPage
        loadUbaStatisticsPage(sqlContext);
        
        //6. Table4: f_terminal_map 
        //load from hbase::src_user_event(Table1) to F_TERMINAL_MAP
        loadFTerminalMap(sqlContext);
        
        //7. Table6: f_statistics_event
        //load from hbase::src_user_event(Table1) to F_STATISTICS_EVENT
//        loadFStatisticsEvent(sqlContext);
        
        //9. Table7: 
        //load from hbase::src_user_event(Table1) to FACR_USER_BEHAVIOR_TELE
        loadFactUserBehaviorTele(sqlContext);
        
        ////////////////////////////////////////////////////////////////
        sqlContext.clearCache();
        sc.stop();
    }
    
    
    public static void loadUbaLogEvent(HiveContext sqlContext, JavaRDD<FactSrcUserEvent> rdd) {
        String FILENAME_WRITE = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_UBALOGEVENT_OUTPUTPATH);
        SrcUserEvent mSrcUserEvent = new SrcUserEvent();
        //1. load from hbase::src_user_event to FACR_LOG_EVENT
        mSrcUserEvent.pairRDD2Parquet(sqlContext, rdd, FILENAME_WRITE);
        //2. load from hbase::src_user_event to FACR_LOG_EVENT for one day (previous day)
        String dailyPath = mSrcUserEvent.getDailyPath(FILENAME_WRITE);
        mSrcUserEvent.pairRDD2ParquetDaily(sqlContext, rdd, dailyPath);
    }
    
    public static void loadUbaUserEvent(HiveContext sqlContext) {
        UserEvent mUserEvent = new UserEvent();
        DataFrame UserEventDF = mUserEvent.getUserEventDF(sqlContext);
        JavaRDD<FactUserEvent> UserEventRDD = mUserEvent.getJavaRDD(UserEventDF);   
        
        //write to parquet
        String USER_EVENT_FILENAME_WRITE = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_UBAUSEREVENT_OUTPUTPATH);
        mUserEvent.pairRDD2Parquet(sqlContext, UserEventRDD, USER_EVENT_FILENAME_WRITE);
        
        //write to hbase
//      JavaPairRDD<ImmutableBytesWritable, Put> hbasePutsUserEvent = mUserEvent.getPut(UserEventRDD);//write to hbase
//      HBaseUtil.putRDD2Hbase(hbasePutsUserEvent, "FACT_USER_EVENT");
        //end
    }
    
    public static void loadUbaStatisticsPage(HiveContext sqlContext) {
        StatisticsPage mStatisticsPage = new StatisticsPage();
        DataFrame StatisticsPageDF = mStatisticsPage.getStatisticsPageDF(sqlContext);
        JavaRDD<FactStatisticsPage> StatisticsPageRDD = mStatisticsPage.getJavaRDD(StatisticsPageDF);
        
        //write to parquet
        String STATISTICS_PAGE_FILENAME_WRITE = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_UBASTATISTICSPAGE_OUTPUTPATH);
        mStatisticsPage.pairRDD2Parquet(sqlContext, StatisticsPageRDD, STATISTICS_PAGE_FILENAME_WRITE);
        
        //write to hbase
//      JavaPairRDD<ImmutableBytesWritable, Put> hbasePutsStatisticsPage = mStatisticsPage.getPut(StatisticsPageRDD);//write to hbase
//      HBaseUtil.putRDD2Hbase(hbasePutsStatisticsPage, "FACT_STATISTICS_PAGE");
        //end
    }
    
    public static void loadFTerminalMap(HiveContext sqlContext) {
        TerminalMap mTerminalMap = new TerminalMap();
        DataFrame TerminalMapDF = mTerminalMap.getTerminalMapInfoResultDF(sqlContext);
        JavaRDD<FactTerminalMap> TerminalMapRDD = mTerminalMap.getJavaRDD(TerminalMapDF);
        
        //write to parquet
        String TERMINAL_MAP_FILENAME_WRITE = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FTERMINALMAP_OUTPUTPATH);
        mTerminalMap.pairRDD2Parquet(sqlContext, TerminalMapRDD, TERMINAL_MAP_FILENAME_WRITE);
    }
    
    public static void loadFStatisticsEvent(HiveContext sqlContext) {
        StatisticsEvent mStatisticsEvent = new StatisticsEvent();
        
        DataFrame StatisticsEventDF = mStatisticsEvent.getStatisticsEventDF(sqlContext);
        JavaRDD<FactStatisticsEvent> StatisticsEventRDD = mStatisticsEvent.getJavaRDD(StatisticsEventDF);
        
        //write to parquet
        String STATISTICS_EVENT_FILENAME_WRITE = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH);
        mStatisticsEvent.pairRDD2Parquet(sqlContext, StatisticsEventRDD, STATISTICS_EVENT_FILENAME_WRITE);
    }
    
    public static void loadFactUserBehaviorTele(HiveContext sqlContext) {
        UserBehaviorTele mUserBehaviorTele = new UserBehaviorTele();
        DataFrame UserBehaviorTeleDF = mUserBehaviorTele.getUserBehaviorTeleDF(sqlContext);
        JavaRDD<FactUserBehaviorTele> UserBehaviorTeleRDD = mUserBehaviorTele.getJavaRDD(UserBehaviorTeleDF);
        
        //write to parquet
        String USER_BEHAVIOR_TRACK_TEL_FILENAME_WRITE = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORTELE_OUTPUTPATH);
        mUserBehaviorTele.pairRDD2Parquet(sqlContext, UserBehaviorTeleRDD, USER_BEHAVIOR_TRACK_TEL_FILENAME_WRITE);
        
        //write to hbase
//      JavaPairRDD<ImmutableBytesWritable, Put> hbasePutsUserBehaviorTele = mUserBehaviorTele.getPut(UserBehaviorTeleRDD);//write to hbase
//      HBaseUtil.putRDD2Hbase(hbasePutsUserBehaviorTele, "FACR_USER_BEHAVIOR_TRACK_TELE");
        //end
    }
    


}
