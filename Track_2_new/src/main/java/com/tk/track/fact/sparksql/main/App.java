package com.tk.track.fact.sparksql.main;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.base.table.BaseTable;
import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.clue.impl.SpringFestivalPurchaseClueImpl;
import com.tk.track.fact.sparksql.desttable.FactNetTeleCallSkill;
import com.tk.track.fact.sparksql.desttable.FactSrcUserEvent;
import com.tk.track.fact.sparksql.desttable.FactStatisticsEvent;
import com.tk.track.fact.sparksql.desttable.FactStatisticsEventWithAnony;
import com.tk.track.fact.sparksql.desttable.FactStatisticsPage;
import com.tk.track.fact.sparksql.desttable.FactTerminalMap;
import com.tk.track.fact.sparksql.desttable.FactUserBehaviorClue;
import com.tk.track.fact.sparksql.desttable.FactUserBehaviorClueScode;
import com.tk.track.fact.sparksql.desttable.FactUserBehaviorClueScode_bw;
import com.tk.track.fact.sparksql.desttable.FactUserBehaviorTele;
import com.tk.track.fact.sparksql.desttable.FactUserEvent;
import com.tk.track.fact.sparksql.desttable.TeleUseCondition;
import com.tk.track.fact.sparksql.desttable.TempTeleAppUseSummary;
import com.tk.track.fact.sparksql.desttable.TempTeleShareSummary;
import com.tk.track.fact.sparksql.desttable.TempUserSmsBehavior;
import com.tk.track.fact.sparksql.desttable.UserSmsConfig;
import com.tk.track.fact.sparksql.etl.AccountWorthSearchService;
import com.tk.track.fact.sparksql.etl.BehaviorCommon;
import com.tk.track.fact.sparksql.etl.ChannelCustomerConversion;
import com.tk.track.fact.sparksql.etl.ChildrenCare;
import com.tk.track.fact.sparksql.etl.EStation2HomeWeb;
import com.tk.track.fact.sparksql.etl.FactUserBehaviorIndex;
import com.tk.track.fact.sparksql.etl.HealthClauseDuration;
import com.tk.track.fact.sparksql.etl.HealthPageService;
import com.tk.track.fact.sparksql.etl.InsuranceProcessClue;
import com.tk.track.fact.sparksql.etl.InsuranceProcessEvent;
//import com.tk.track.fact.sparksql.etl.InsuranceRenewal;
import com.tk.track.fact.sparksql.etl.IntegeralMall;
import com.tk.track.fact.sparksql.etl.MobileAccountWorthSearchService;
import com.tk.track.fact.sparksql.etl.NetTeleActiveUserApp;
import com.tk.track.fact.sparksql.etl.NetTeleCallSkill;
import com.tk.track.fact.sparksql.etl.NetTeleOldCoustomer;
import com.tk.track.fact.sparksql.etl.RecomengWithoutAlgo;
import com.tk.track.fact.sparksql.etl.RecommendWithoutAlgoEvaluate;
import com.tk.track.fact.sparksql.etl.RecommendWithoutAlgoEvaluate_new2;
import com.tk.track.fact.sparksql.etl.SeedIdProductAnalyse;
import com.tk.track.fact.sparksql.etl.SrcUserEvent;
import com.tk.track.fact.sparksql.etl.StatisticsEvent;
import com.tk.track.fact.sparksql.etl.StatisticsEventAnon;
import com.tk.track.fact.sparksql.etl.StatisticsEventCallSkill;
import com.tk.track.fact.sparksql.etl.StatisticsPage;
import com.tk.track.fact.sparksql.etl.TeleAppUseSummary;
import com.tk.track.fact.sparksql.etl.TeleShareSummary;
import com.tk.track.fact.sparksql.etl.TeleSystemUseCondition;
import com.tk.track.fact.sparksql.etl.TerminalMap;
import com.tk.track.fact.sparksql.etl.TerminalMapApp;
import com.tk.track.fact.sparksql.etl.UserBehaviorActivitiesAccMonth4;
import com.tk.track.fact.sparksql.etl.UserBehaviorActivitiesMonth6;
import com.tk.track.fact.sparksql.etl.UserBehaviorBrowseCar;
import com.tk.track.fact.sparksql.etl.UserBehaviorBrowseWebsite;
import com.tk.track.fact.sparksql.etl.UserBehaviorClaimFlow;
import com.tk.track.fact.sparksql.etl.UserBehaviorClueCommon;
import com.tk.track.fact.sparksql.etl.UserBehaviorCountPremium;
import com.tk.track.fact.sparksql.etl.UserBehaviorFlowGoodsDetail;
import com.tk.track.fact.sparksql.etl.UserBehaviorH5Insureflow;
import com.tk.track.fact.sparksql.etl.UserBehaviorJulyActivity;
import com.tk.track.fact.sparksql.etl.UserBehaviorTele;
import com.tk.track.fact.sparksql.etl.UserBehaviorTeleApp;
import com.tk.track.fact.sparksql.etl.UserBehaviorTeleH5Act;
import com.tk.track.fact.sparksql.etl.UserBehaviorTeleWap;
import com.tk.track.fact.sparksql.etl.UserBehaviorTeleWebsiteAct;
import com.tk.track.fact.sparksql.etl.UserBehaviorTeleWechatHot;
import com.tk.track.fact.sparksql.etl.UserBehaviorTeleWechatInsureflow;
import com.tk.track.fact.sparksql.etl.UserBehaviorTylp;
import com.tk.track.fact.sparksql.etl.UserBehaviorWechatFreeCheck;
import com.tk.track.fact.sparksql.etl.UserBehaviorWechatHealthTest;
import com.tk.track.fact.sparksql.etl.UserBehaviorWechatInsureflow_bwMedical;
import com.tk.track.fact.sparksql.etl.UserBehaviorWechatMy;
import com.tk.track.fact.sparksql.etl.UserBehaviorYlsq;
import com.tk.track.fact.sparksql.etl.UserEvent;
import com.tk.track.fact.sparksql.etl.UserPolicyInfo;
import com.tk.track.fact.sparksql.etl.UserSmsBehavior;
import com.tk.track.fact.sparksql.etl.WeChatActivity;
import com.tk.track.fact.sparksql.etl.WechatActivityClueCommon;
import com.tk.track.fact.sparksql.etl.WomenDayWeChat;
import com.tk.track.fact.sparksql.util.DMUtility;
import com.tk.track.fact.sparksql.util.HBaseUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;
import com.tk.track.util.TimeUtil;
import com.tk.track.vo.UserBehaviorConfigVo;


/*
 * spark-submit --class com.tk.healthscore.fact.sparksql.main.App --master yarn /home/renxiaodong/spark/sample/fact.jar
 */
public class App {
    private static final String CVD_TABLE_NAME = "TMP_CALCULATE_VISIT_DURATION";
    private static final String CVI_TABLE_NAME = "TMP_CALCULATE_VISIT_INFO";
    private static final String FD_TABLE_NAME = "TMP_FILL_DURATION";
    private static final String FACT_USERBEHAVIOR_INDEX = "FACT_USERBEHAVIOR_INDEX";
    private static final String FACT_TERMINAL_MAP_APP = "FACT_TERMINAL_MAP_APP";
    private static final String FACT_SEED_PRODUCT = "FACT_SEED_PRODUCT";
    private static final String RecommendWithoutAlgoBase = "Recommend_Without_Algo_Base";

    public static void main(String[] args) {
        /*SparkConf conf = new SparkConf().setAppName("Taikang Track ETL TOTAL");
        conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, 
                TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
        SparkContext sc = new SparkContext(conf);

        HiveContext sqlContext = new HiveContext(sc);
        RandomUDF.genrandom(sc, sqlContext);
        
        //1. read log file for raw data
        // Table1: uba_log_event 
        // load from hbase::src_user_event to FACR_LOG_EVENT
        loadRawData(sc, sqlContext, true);
        
        //3. Table2: uba_user_event
        //load from hbase::src_user_event(Table1) to FACR_USER_EVENT
        loadUbaUserEvent(sqlContext);

        //4. Table3: uba_statistics_page 
        //load from hbase::src_user_event(Table1) to FactStatisticsPage
        loadUbaStatisticsPage(sqlContext);
        
        //5. Table4: f_terminal_map 
        //load from hbase::src_user_event(Table1) to F_TERMINAL_MAP
        loadFTerminalMap(sqlContext);
        
        //6. Table6: f_statistics_event
        //load from hbase::src_user_event(Table1) to F_STATISTICS_EVENT
        loadFStatisticsEvent(sqlContext);
        DeleteCachetable();
        
        //7. Table7: fact_user_behavior_tele
        //load from hbase::src_user_event(Table1) to FACR_USER_BEHAVIOR_TELE
        loadFactUserBehaviorTele(sqlContext);
        
        ////////////////////////////////////////////////////////////////
        sqlContext.clearCache();
        sc.stop();*/
    	
    	
    	
    	
    }
    
    public static void loadRawData(SparkContext sparkContext, HiveContext sqlContext, boolean isNeedSave) {
        if (isNeedSave) {
            // read log file
        	///user/tkonline/taikangtrack/temp/tracklog 输入的路径 ，不用改
            String FILENAME_READ = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_TRACKLOG_INPUTPATH);
            SrcUserEvent mSrcUserEvent = new SrcUserEvent();
            JavaRDD<String> lines = sparkContext.textFile(FILENAME_READ, 1).toJavaRDD();
            // filter the raw data
            JavaRDD<FactSrcUserEvent> logRDD = mSrcUserEvent.getJavaRDD(lines);
            
            //2. Table1: uba_log_event 
            //load from hbase::src_user_event to FACR_LOG_EVENT
            loadUbaLogEvent(sqlContext, logRDD);
            //registry
            sqlContext.createDataFrame(logRDD, FactSrcUserEvent.class).registerTempTable("Temp_SrcUserEvent");
        } else {
        	String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH);
        	String FILENAME_WRITE = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_UBALOGEVENT_OUTPUTPATH);
        	if (TK_DataFormatConvertUtil.isExistsPath(path)) {
        		SrcUserEvent mSrcUserEvent = new SrcUserEvent();
        		String dailyPath = mSrcUserEvent.getDailyPath(FILENAME_WRITE);
        		sqlContext.load(dailyPath).registerTempTable("Temp_SrcUserEvent");
        	} else {
        		sqlContext.load(FILENAME_WRITE).registerTempTable("Temp_SrcUserEvent");
        	}
        }
    }
    
    public static void loadUbaLogEvent(HiveContext sqlContext, JavaRDD<FactSrcUserEvent> rdd) {
        // /user/tkonline/taikangtrack/data_test/uba_log_event 输出的路径改成测试环境
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
    /**
     * system002
     * 网电系统的使用情况
     * @param sqlContext
     * @param appids
     */
    public static void loadTeleSystemUseCondition(HiveContext sqlContext,String appids) {
         TeleSystemUseCondition tUserCondition=new TeleSystemUseCondition();
         SrcUserEvent mSrcUserEvent = new SrcUserEvent();
         // /user/tkonline/taikangtrack/data/uba_log_event
     	String FILENAME_WRITE = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_UBALOGEVENT_OUTPUTPATH);
     	//拼上路径和昨天的时间
     	String dailyPath = mSrcUserEvent.getDailyPath(FILENAME_WRITE);
     	//吧 uba_log_event 注册到大数据平台的临时表
 		sqlContext.load(dailyPath).registerTempTable("Temp_teleUseCondition");
        DataFrame StatisticsEventDF = tUserCondition.getTeleSystemUseConditionDF(sqlContext,appids);
        if(StatisticsEventDF != null){
        	 JavaRDD<TeleUseCondition> teleUseConditionRDD = tUserCondition.getJavaRDD(StatisticsEventDF);
        	 String TELEUSECONDITION_OUTPUTPATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_TELEUSECONDITION_OUTPUTPATH);
        	 tUserCondition.pairRDD2Parquet(sqlContext, teleUseConditionRDD, TELEUSECONDITION_OUTPUTPATH);
        }
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
    
    public static void loadFTerminalMapApp(HiveContext sqlContext,boolean isCreate,boolean isCache) {
    	DataFrame df = null;
        //write to parquet
        String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FTERMINALMAPAPP_OUTPUTPATH);
        String sysdt = GetSysDate(0);
		path = path + "-" + sysdt;
		if(isCreate){
			TerminalMapApp mTerminalMap = new TerminalMapApp();
	        df = mTerminalMap.getTerminalMapInfoResultDF(sqlContext);
	        mTerminalMap.saveAsParquet(df, path);
			String rowkeyStr = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FACTTERMINALMAPAPP_ROWKEY);
			updateHbase(rowkeyStr,sysdt);
		}else{
			df = sqlContext.load(path);
			df.registerTempTable(FACT_TERMINAL_MAP_APP);
			if(isCache)
				sqlContext.cacheTable(FACT_TERMINAL_MAP_APP);
 		}
    }
        
    public static void loadFStatisticsEvent(HiveContext sqlContext) {
        StatisticsEvent mStatisticsEvent = new StatisticsEvent();
        DataFrame StatisticsEventDF = mStatisticsEvent.getStatisticsEventDF(sqlContext);
        JavaRDD<FactStatisticsEvent> StatisticsEventRDD = mStatisticsEvent.getJavaRDD(StatisticsEventDF);
        
        //write to parquet
        String STATISTICS_EVENT_FILENAME_WRITE = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH);
        mStatisticsEvent.pairRDD2Parquet(sqlContext, StatisticsEventRDD, STATISTICS_EVENT_FILENAME_WRITE);
    }
    public static void loadFStatisticsEventWithAnony(HiveContext sqlContext) {
    	StatisticsEventAnon mStatisticsEvent = new StatisticsEventAnon();
        DataFrame StatisticsEventDF = mStatisticsEvent.getStatisticsEventDFAnony(sqlContext);
        JavaRDD<FactStatisticsEventWithAnony> StatisticsEventRDD = mStatisticsEvent.getJavaRDD(StatisticsEventDF);
        
        //write to parquet
        String STATISTICS_EVENT_FILENAME_WRITE = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_ANONY_OUTPUTPATH);
        DataFrame df=sqlContext.createDataFrame(StatisticsEventRDD, FactStatisticsEventWithAnony.class);

        df.registerTempTable("TMP_FACT_STATISTICS_EVENT");
        mStatisticsEvent.saveAsParquet(df,STATISTICS_EVENT_FILENAME_WRITE);
//        mStatisticsEvent.pairRDD2Parquet(sqlContext, StatisticsEventRDD, STATISTICS_EVENT_FILENAME_WRITE);
    }
    
    public static void loadCallSkillStatisticsEvent(HiveContext sqlContext,String appids) {
    	StatisticsEventCallSkill mStatisticsEvent = new StatisticsEventCallSkill();
    	
		SrcUserEvent mSrcUserEvent = new SrcUserEvent();
    	String FILENAME_WRITE = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_UBALOGEVENT_OUTPUTPATH);
    	String dailyPath = mSrcUserEvent.getDailyPath(FILENAME_WRITE);
		sqlContext.load(dailyPath).registerTempTable("Temp_SrcUserEvent");

        DataFrame StatisticsEventDF = mStatisticsEvent.getStatisticsEventDF(sqlContext,appids);
        JavaRDD<FactStatisticsEvent> StatisticsEventRDD = mStatisticsEvent.getJavaRDD(StatisticsEventDF);
        
        //write to parquet
        String STATISTICS_EVENT_FILENAME_WRITE = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_CALLSKILL_OUTPUTPATH);
        mStatisticsEvent.pairRDD2Parquet(sqlContext, StatisticsEventRDD, STATISTICS_EVENT_FILENAME_WRITE);
    }
    
    //mall001
    public static void loadFactUserBehaviorTele(HiveContext sqlContext) {
    	UserBehaviorTele mUserBehaviorTele = new UserBehaviorTele();
        DataFrame UserBehaviorTeleDF = mUserBehaviorTele.getUserBehaviorTeleDF(sqlContext);
        JavaRDD<FactUserBehaviorTele> UserBehaviorTeleRDD = mUserBehaviorTele.getJavaRDD(UserBehaviorTeleDF);
        
        //write to parquet
        String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORTELE_OUTPUTPATH);
        mUserBehaviorTele.pairRDD2Parquet(sqlContext, UserBehaviorTeleRDD, path);
        repatition(sqlContext, path);
    }
    
    public static void loadUserPolicyInfo(HiveContext sqlContext) {
        UserPolicyInfo mUserPolicyInfo = new UserPolicyInfo();
        //write to parquet
        String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERPOLICYINFO_OUTPUTPATH);
        saveAsParquetByDay(sqlContext,mUserPolicyInfo, path);
    }
    
    private static void saveAsParquetByDay(HiveContext sqlContext, UserPolicyInfo mUserPolicyInfo,String path) {
    	String sysdt = GetSysDate(0);
    	DataFrame df = mUserPolicyInfo.getUserPolicyInfoDF(sqlContext);
		path = path + "-" + sysdt;
		mUserPolicyInfo.saveAsParquet(df, path);
		//HBASE UPDATE
		String tableName=TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FACTNETTELERESULTINFO);
		byte[] rowkey=Bytes.toBytes(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FACTUSERPOLICYINFO_ROWKEY));
		byte[] family=Bytes.toBytes(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY)); 
		byte[] qualifier=Bytes.toBytes(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_QUALIFIER)); 
		byte[] value=Bytes.toBytes(sysdt);
		HBaseUtil.put(tableName,rowkey,family,qualifier,value);
	}
    
    //WebsiteAct
    public static void loadFactUserBehaviorTeleWebsiteAct(HiveContext sqlContext,String appids) {
        UserBehaviorTeleWebsiteAct mUserBehaviorTele = new UserBehaviorTeleWebsiteAct();
        DataFrame UserBehaviorTeleDF = mUserBehaviorTele.getUserBehaviorTeleDF(sqlContext, appids);
        
        if(UserBehaviorTeleDF!=null){
	        JavaRDD<FactUserBehaviorTele> UserBehaviorTeleRDD = mUserBehaviorTele.getJavaRDD(UserBehaviorTeleDF);
	        //write to parquet
	        String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORTELE_OUTPUTPATH);
	        mUserBehaviorTele.pairRDD2Parquet(sqlContext, UserBehaviorTeleRDD, path);
	        repatition(sqlContext, path);
        }
    }
    
    //H5Act
    public static void loadFactUserBehaviorTeleH5Act(HiveContext sqlContext,String appids) {
    	UserBehaviorTeleH5Act mUserBehaviorTele = new UserBehaviorTeleH5Act();
        DataFrame UserBehaviorTeleDF = mUserBehaviorTele.getUserBehaviorTeleDF(sqlContext, appids);
        
        if(UserBehaviorTeleDF!=null){
	        JavaRDD<FactUserBehaviorTele> UserBehaviorTeleRDD = mUserBehaviorTele.getJavaRDD(UserBehaviorTeleDF);
	        
	        //write to parquet
	        String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORTELE_OUTPUTPATH);
	        mUserBehaviorTele.pairRDD2Parquet(sqlContext, UserBehaviorTeleRDD, path);
	        repatition(sqlContext, path);
        }
    }
    
    //Wap
    public static void loadFactUserBehaviorTeleWap(HiveContext sqlContext,String appids) {
    	UserBehaviorTeleWap mUserBehaviorTele = new UserBehaviorTeleWap();
        DataFrame UserBehaviorTeleDF = mUserBehaviorTele.getUserBehaviorTeleDF(sqlContext, appids);
        
        if(UserBehaviorTeleDF!=null){
	        JavaRDD<FactUserBehaviorTele> UserBehaviorTeleRDD = mUserBehaviorTele.getJavaRDD(UserBehaviorTeleDF);
	        
	        //write to parquet
	        String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORTELE_OUTPUTPATH);
	        mUserBehaviorTele.pairRDD2Parquet(sqlContext, UserBehaviorTeleRDD, path);
	        repatition(sqlContext, path);
        }
    }

    //新WechatHot(新app7规则)
    public static void newLoadFactUserBehaviorTeleWechatHot(HiveContext sqlContext,String appids){
        UserBehaviorTeleWechatHot mUserBehaviorTele = new UserBehaviorTeleWechatHot();
        DataFrame UserBehaviorTeleDF = mUserBehaviorTele.getNewUserBehaviorTeleDF(sqlContext, appids);
        if(UserBehaviorTeleDF!=null){
            System.out.println("start==========="+UserBehaviorTeleDF.count()+"里面count数=========end");
            JavaRDD<FactUserBehaviorTele> UserBehaviorTeleRDD = mUserBehaviorTele.getJavaRDD(UserBehaviorTeleDF);
            ///user/tkonline/taikangtrack/data_test/fact_user_behavior_tele
            String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORTELE_OUTPUTPATH);
            mUserBehaviorTele.pairRDD2Parquet(sqlContext, UserBehaviorTeleRDD, path);
            repatition(sqlContext, path);
        }

    }

    //WechatHot
    public static void loadFactUserBehaviorTeleWechatHot(HiveContext sqlContext,String appids) {
    	UserBehaviorTeleWechatHot mUserBehaviorTele = new UserBehaviorTeleWechatHot();
        DataFrame UserBehaviorTeleDF = mUserBehaviorTele.getUserBehaviorTeleDF(sqlContext, appids);
        if(UserBehaviorTeleDF!=null){
	        JavaRDD<FactUserBehaviorTele> UserBehaviorTeleRDD = mUserBehaviorTele.getJavaRDD(UserBehaviorTeleDF);
	        String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORTELE_OUTPUTPATH);
	        mUserBehaviorTele.pairRDD2Parquet(sqlContext, UserBehaviorTeleRDD, path);
	        repatition(sqlContext, path);
        }
    }
    
    //网电app活跃用户
    public static void loadAppActiveUser(HiveContext sqlContext){
    	Date date = new Date();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		String formatdate = format.format(date);
		
    	NetTeleActiveUserApp netTeleActiveUserApp = new NetTeleActiveUserApp();
    	DataFrame UserBehaviorTeleDF = netTeleActiveUserApp.getNetTeleActiveUser(sqlContext);
    	if(UserBehaviorTeleDF!=null){
    		JavaRDD<FactUserBehaviorClue> userBehaviorTeleRDDJavaRDD = netTeleActiveUserApp.getJavaRDD(UserBehaviorTeleDF);
    		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_APPACTIVEUSER_OUTPUTPATH)+formatdate; 
//    		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORCLUE_OUTPUTPATH);
    		netTeleActiveUserApp.pairRDD2Parquet(sqlContext, userBehaviorTeleRDDJavaRDD, path);
    		repatition(sqlContext, path);
    	}//TAIKANG_HDFS_FACTUSERBEHAVIORCLUE_OUTPUTPATH
    	
    }
    

    //京鄂鲁老客户
    public static void loadOldCoustomer(HiveContext sqlContext){
    	NetTeleOldCoustomer oldCoustomer = new NetTeleOldCoustomer();
    	DataFrame df = oldCoustomer.getOldCoustomer(sqlContext);
    	
    	if(df != null){
    		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_OLDCOUSTOMER_OUTPUTPATH);
    		df.save(path, "parquet", SaveMode.Append);
    		repatition(sqlContext, path);
    	}
    }
    
    //财险一年期续保
//    public static void oneyearRenewal(HiveContext sqlContext){
//    	InsuranceRenewal renewal = new InsuranceRenewal();
//    	DataFrame df = renewal.getOneYearRenewal(sqlContext);
//    	
//    	if(df != null){
//    		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_ONEYEARRENEWAL_OUTPUTPATH);
//    		df.save(path, "parquet", SaveMode.Append);
//    		repatition(sqlContext, path);
//    	}
//    }
    
    public static void loadFactUserBehaviorTeleWechatInsureflow(HiveContext sqlContext,String appids) {
    	UserBehaviorTeleWechatInsureflow mUserBehaviorTele = new UserBehaviorTeleWechatInsureflow();
        DataFrame UserBehaviorTeleDF = mUserBehaviorTele.getUserBehaviorTeleDF(sqlContext, appids);
        if(UserBehaviorTeleDF!=null){
	        JavaRDD<FactUserBehaviorTele> UserBehaviorTeleRDD = mUserBehaviorTele.getJavaRDD(UserBehaviorTeleDF);
	        String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORTELE_OUTPUTPATH);
	        mUserBehaviorTele.pairRDD2Parquet(sqlContext, UserBehaviorTeleRDD, path);
	        repatition(sqlContext, path);
        }
    }
    
    public static void loadH5Insureflow(HiveContext sqlContext,String appids) {
    	UserBehaviorH5Insureflow mUserBehaviorH5 = new UserBehaviorH5Insureflow();
        DataFrame userBehaviorH5DF = mUserBehaviorH5.getUserBehaviorH5DF(sqlContext, appids);
        	System.out.println("userBehaviorH5DF: ");
        if(userBehaviorH5DF!=null){
	        JavaRDD<FactUserBehaviorClueScode> UserBehaviorH5RDD = mUserBehaviorH5.getJavaRDD(userBehaviorH5DF);
	        System.out.println("userBehaviorH5DF1: ");
//	        String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORSCODECLUE_OUTPUTPATH);
	        String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORCLUE_OUTPUTPATH);
	        mUserBehaviorH5.pairRDD2Parquet(sqlContext, UserBehaviorH5RDD, path);
	        System.out.println("userBehaviorH5DF2: ");
	        repatition(sqlContext, path);
	        System.out.println("userBehaviorH5DF3: ");
        }
    }
    public static void loadWechatInsureflow(HiveContext sqlContext,String appids) {
    	UserBehaviorWechatInsureflow_bwMedical mUserBehavior = new UserBehaviorWechatInsureflow_bwMedical();
        DataFrame userBehaviorH5DF = mUserBehavior.getUserBehaviorWechatDF(sqlContext, appids);
        if(userBehaviorH5DF!=null){
        	JavaRDD<FactUserBehaviorClueScode_bw> UserBehaviorH5RDD = mUserBehavior.getJavaRDD(userBehaviorH5DF);
	        String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORTELE_BWMEDICAL_OUTPUTPATH);
	        mUserBehavior.pairRDD2Parquet(sqlContext, UserBehaviorH5RDD, path);
	        repatition(sqlContext, path);
        }
    }
    //AccountWorhtSearch
    public static void loadFactUserBehaviorClueAccountWorthSearch(HiveContext sqlContext,String appids) {
    	AccountWorthSearchService accountWorthSearchService = new AccountWorthSearchService();
    	DataFrame userBehaviorClueDF = accountWorthSearchService.getUserBehaviorClueDF(sqlContext, appids);
    	
    	if(userBehaviorClueDF != null){
    		JavaRDD<FactUserBehaviorClue> userBehaviorClueRDD = accountWorthSearchService.getJavaRDD(userBehaviorClueDF);
    		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORCLUE_OUTPUTPATH);
    		accountWorthSearchService.pairRDD2Parquet(sqlContext, userBehaviorClueRDD, path);
    		repatition(sqlContext, path);
    	}
    }
    
    //App
//    public static void loadFactUserBehaviorTeleApp(HiveContext sqlContext,String appids) {
//    	UserBehaviorTeleApp mUserBehaviorTele = new UserBehaviorTeleApp();
//        DataFrame UserBehaviorTeleDF = mUserBehaviorTele.getUserBehaviorTeleDF(sqlContext, appids);
//        
//        if(UserBehaviorTeleDF!=null){
//	        JavaRDD<FactUserBehaviorTele> UserBehaviorTeleRDD = mUserBehaviorTele.getJavaRDD(UserBehaviorTeleDF);
//	        
//	        //write to parquet
//	        String USER_BEHAVIOR_TRACK_TEL_FILENAME_WRITE = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORTELE_OUTPUTPATH);
//	        mUserBehaviorTele.pairRDD2Parquet(sqlContext, UserBehaviorTeleRDD, USER_BEHAVIOR_TRACK_TEL_FILENAME_WRITE);
//        }
//    }
    
    //CallSkill
    public static void loadNetTeleCallSkill(HiveContext sqlContext,String appids) {
    	NetTeleCallSkill netTeleCallSkill = new NetTeleCallSkill();
        DataFrame netTeleCallSkillDF = netTeleCallSkill.getNetTeleCallSkillDF(sqlContext, appids);
        
        if(netTeleCallSkillDF!=null){
	        JavaRDD<FactNetTeleCallSkill> netTeleCallSkillRDD = netTeleCallSkill.getJavaRDD(netTeleCallSkillDF);
	        
	        //write to parquet
	        String NETTELECALLSKILL_OUTPUTPATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_NETTELECALLSKILL_OUTPUTPATH);
	        netTeleCallSkill.pairRDD2Parquet(sqlContext, netTeleCallSkillRDD, NETTELECALLSKILL_OUTPUTPATH);
        }
    }
    
    //原生App线索生成
    public static void loadFactUserBehaviorTeleApp(HiveContext sqlContext) {
    	UserBehaviorTeleApp tele = new UserBehaviorTeleApp();
        DataFrame df = tele.getUserBehaviorTeleDF(sqlContext);
        if(df!=null){
	        JavaRDD<FactUserBehaviorTele> UserBehaviorTeleRDD = tele.getJavaRDD(df);
	        String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORTELE_OUTPUTPATH);
	        tele.pairRDD2Parquet(sqlContext, UserBehaviorTeleRDD, path);
	        repatition(sqlContext, path);//重分区
        }
        
    }
    /*
     * 积分商城线索
     */
    
    public static void loadFactIntegeralMallApp(HiveContext sqlContext,String appids) {
    	IntegeralMall integeralMall = new IntegeralMall();
    	DataFrame df = integeralMall.getIntegeralMallDF(sqlContext,appids);
    	if(df!=null){
    		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORCLUE_OUTPUTPATH);
    		df.save(path, "parquet", SaveMode.Append);
    		repatition(sqlContext, path);
    	}
    	
    }
      /*
     * 38妇女节线索
     */
    public static void loadWomenDayWeChatApp(HiveContext sqlContext) {
        WomenDayWeChat womenDayWeChat = new WomenDayWeChat();
        DataFrame df =womenDayWeChat.getWomenDayWeChatDF(sqlContext);
        if(df!=null){
            String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERWOMENDAYWECHAT_OUTPUTPATH);
            System.out.println(path);
            //删除原先数据
            DMUtility.deletePath(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERWOMENDAYWECHAT_OUTPUTPATH);
            df.save(path, "parquet", SaveMode.Append);
            repatition(sqlContext, path);
        }

    }
    /*
     * 微信短期活动通用入口
     * 
     */
    public static void loadWeChatActivityApp(HiveContext sqlContext) {
        WeChatActivity womenDayWeChat = new WeChatActivity();
        DataFrame df =womenDayWeChat.getWeChatActivityDF(sqlContext);
        if(df!=null){
            String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_WECHATACTIVITY_USUAL_PATH);
            System.out.println(path);
            //删除原先数据
            DMUtility.deletePath(TK_DatabaseValues.TAIKANG_WECHATACTIVITY_USUAL_PATH);
            df.save(path, "parquet", SaveMode.Append);
            repatition(sqlContext, path);
        }

    }
    /*
     * 
     */
    
    public static void loadFactChildrenCareApp(HiveContext sqlContext,String appids,String clueyear) {
    	ChildrenCare childrenCare = new ChildrenCare();
    	DataFrame df = childrenCare.getChildrenCareDF(sqlContext,appids);
//    	DataFrame otherdf = childrenCare.getChildrenCareOthersDF(sqlContext,appids,clueyear);
    	
    	if(df!=null){
    		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTCHILDRENCARECLUE_OUTPUTPATH);
    		df.save(path, "parquet", SaveMode.Append);
    		repatition(sqlContext, path);
    	}
    	
//    	if(otherdf!=null){
//    		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTOTHERCHILDRENCARECLUE_OUTPUTPATH);
//    		otherdf.save(path, "parquet", SaveMode.Append);
//    		repatition(sqlContext, path);
//    	}
    	
    }
   
    
    
    /**
     * @param 通用理赔线索
     */
    public static void loadUserBehaviorTylp(HiveContext sqlContext){
    	UserBehaviorTylp tylp=new UserBehaviorTylp();
    	tylp.createConcatUdf(sqlContext);
        tylp.createConcatUdf3(sqlContext);
    	tylp.createTyDf(sqlContext);
    	DataFrame df=tylp.getUserBehaviorTylp(sqlContext);
    	DataFrame dfother=tylp.getUserBehaviorTylpOther(sqlContext);
		String path=TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_WH_FACTUSERBEHAVIORCLUE_OUTPUTPATH);
    	if(df!=null){
    	
    		tylp.saveasParquet(df, path);

    	}
    	if(dfother!=null){
    		tylp.saveasParquet(dfother, path);

    	}
    	repatition(sqlContext, path);
    }
    
    
    
    /**
     * @param 官网页面
     */
    public static void loadUserBehaviorBrowseWebsite(HiveContext sqlContext){
    	UserBehaviorBrowseWebsite browser=new UserBehaviorBrowseWebsite();
    	browser.loadPolicyInfoResultTable(sqlContext);
    	DataFrame df=browser.getBrowseWebsiteDf(sqlContext);
    	String path=TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORBROWSERWEBSITE_OUTPUTPATH);
    	if(df!=null){
    		browser.saveasParquet(df, path);
    		repatition(sqlContext, path);
    	}
    }
    
    
    
    
    
    
    
    
    
	/**
	 * 创建或者加载 SeedProduct，
	 * 创建时每天生成全量数据，保存成 parquet-日期 形式
	 * @param sqlContext
	 * @param isCreate
	 * @param isCache
	 */
	public static void loadSeedProduct(HiveContext sqlContext,boolean isCreate,boolean isCache) {
		DataFrame df=null;
		//write to parquet
		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_SEEDPRODUCT_OUTPUTPATH);
		String sysdt = GetSysDate(0);
		path = path + "-" + sysdt;
		if(isCreate){
			SeedIdProductAnalyse sipa=new SeedIdProductAnalyse();
			 df=sipa.getExtPageLoadDF(sqlContext);
			if(df!=null){
				//解析日志并生成所需要parquet
//				DataFrame seedDf=sipa.analyseSeedIdProduct(df,sqlContext); //此为一期需求，以下行为新版
				DataFrame seedDf=sipa.getSeedIdProductAnalyse(sqlContext, df);
				sipa.saveAsParquet(seedDf,path);
				String rowkeyStr = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FACTSEEDPRODUCT_ROWKEY);
				updateHbase(rowkeyStr, sysdt);
			}else{
				System.out.println("df = null");
			}
		}else{
			df = sqlContext.load(path);
			df.registerTempTable(FACT_SEED_PRODUCT);
			if(isCache)
				sqlContext.cacheTable(FACT_SEED_PRODUCT);
		}
	}

    
    
/**
 * 养老社区线索生成
 * @Description: 
 * @param sqlContext
 * @param appids
 * @author itw_qiuzq01
 * @date 2016年9月12日
 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
 */
    public static void loadFactUserBehaviorYlsq(HiveContext sqlContext, String appids) {
    	UserBehaviorYlsq ylsq = new UserBehaviorYlsq();
        DataFrame df = ylsq.getUserBehaviorYlsqDF(sqlContext, appids);
        if(df!=null){
	        String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORTELE_OUTPUTPATH);
	        
	        //write to parquet
	        df.save(path, "parquet", SaveMode.Append);
        }
        
    }
    
    /**
     * 通用理赔流程线索生成
     * @Description: 
     * @param sqlContext
     * @param appids
     * @author itw_qiuzq01
     * @date 2016年9月24日
     * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    public static void loadFactUserBehaviorClaimFlow(HiveContext sqlContext, String appids) {
    	UserBehaviorClaimFlow cf = new UserBehaviorClaimFlow();
        DataFrame df = cf.getUserBehaviorClaimFlowDF(sqlContext, appids);
        if(df!=null){
	        String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORCLAIMFLOW_OUTPUTPATH);
	        
	        //write to parquet
	        df.save(path, "parquet", SaveMode.Append);
	        repatition(sqlContext, path);
        }
        
    }
    
    /*
     * 将中间结果和推送线索表导入parquet文件中
     */
    
    public static void loadFactUserBehaviorWechatHealthTest(HiveContext sqlContext, String appids) {
    	UserBehaviorWechatHealthTest weht = new UserBehaviorWechatHealthTest();
    	DataFrame df_sdui = weht.getSrcDataWithUserInfoDF(sqlContext, appids);
        DataFrame df = weht.getUserBehaviorWechatHealthTestDF(sqlContext);
        
        
        if(df_sdui!=null){
	        String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_WH_SRCDATAWITHUSERINFO_OUTPUTPATH);
	        
	        //write to parquet
	        df_sdui.save(path, "parquet", SaveMode.Append);
	        repatition(sqlContext, path);
        }
        
        
        if(df!=null){
	        String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_WH_FACTUSERBEHAVIORCLUE_OUTPUTPATH);
	        
	        //write to parquet
	        df.save(path, "parquet", SaveMode.Append);
	        repatition(sqlContext, path);
        }
        
      
        
    }
    
    

    /**
     * 微信【我的】ETL解析 
     * @Description: 
     * @param sqlContext
     * @author itw_qiuzq01
     * @date 2016年10月11日
     * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    public static void loadFactUserBehaviorWechatMy(HiveContext sqlContext) {
    	UserBehaviorWechatMy we = new UserBehaviorWechatMy();
        DataFrame df = we.getUserBehaviorWechatMyDF(sqlContext);
        if(df!=null){
	        String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORWECHATMY_OUTPUTPATH);
	        
	        //write to parquet
	        df.save(path, "parquet", SaveMode.Append);
	        repatition(sqlContext, path);
        }
        
    }
	
	    /**
	 * 创建或者加载Recommend_Without_Algo_Base，
	 * 创建时每天生成全量数据，保存成 parquet-日期 形式
	 * @param sqlContext
	 * @param isCreate
	 * @param isCache
	 */
	public static void loadRecommendWithoutAlgoBase(HiveContext sqlContext,boolean isCreate,boolean isCache) {
		DataFrame df=null;
		//write to parquet
		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_OUTPUTPATH);
		String sysdt = GetSysDate(0);
		path = path + "-" + sysdt;
		if(isCreate){
			RecomengWithoutAlgo rwa=new RecomengWithoutAlgo();
			 df=rwa.getRecommendDataResultDF(sqlContext);
			if(df!=null){
				rwa.saveAsParquet(df,path);
			}else{
				System.out.println("df = null");
			}
		}else{
			df = sqlContext.load(path);
			df.registerTempTable(RecommendWithoutAlgoBase);
			if(isCache)
				sqlContext.cacheTable(RecommendWithoutAlgoBase);
		}
	}
	 /**
		 * 创建或者加载TAIKANG_HDFS_RECOMMONDWITHOUTALGOSCORE，
		 * 创建时每天生成全量数据，保存成 parquet-日期 形式
		 * @param sqlContext
		 * @param isCreate
		 * @param isCache
		 */
		public static DataFrame loadRecommendWithoutAlgoResult(HiveContext sqlContext,boolean isCreate,boolean isCache) {
			DataFrame df=null;
			//write to parquet
			String path =  TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGOSCORE_OUTPUTPATH);
			String sysdt = GetSysDate(0);
			path = path + "-" + sysdt;
			if(isCreate){
				RecomengWithoutAlgo rwa=new RecomengWithoutAlgo();
				 df=rwa.getRecommendResult(sqlContext);
				if(df!=null){
					rwa.saveAsParquet(df,path);
				}else{
					System.out.println("df = null");
				}
			}else{
				df = sqlContext.load(path);
				df.registerTempTable("RecommendWithoutAlgoResult");
				if(isCache)
					sqlContext.cacheTable("RecommendWithoutAlgoResult");
			}
            return df;
		}

    /**
     * 加载或生成默认产品浏览排名
     * @param sqlContext
     * @param isCreate
     * @param isCache
     */
		public static void loadDefaultRecommendWithoutAlgoResult(HiveContext sqlContext,boolean isCreate,boolean isCache) {
			DataFrame df=null;
			//write to parquet
			String path =  TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGORESULTDEFAULT_OUTPUTPATH);
			String sysdt = GetSysDate(0);
			path = path + "-" + sysdt;
			if(isCreate){
				RecomengWithoutAlgo rwa=new RecomengWithoutAlgo();
				 df=rwa.countTotalDefaultProductPv(sqlContext);
				if(df!=null){
					rwa.saveAsParquet(df,path);
				}else{
					System.out.println("df = null");
				}
			}else{
				df = sqlContext.load(path);
				df.registerTempTable("RecommendWithoutAlgoDefaultResult");
				if(isCache)
					sqlContext.cacheTable("RecommendWithoutAlgoDefaultResult");
			}
		}

    /**
     * 生成或者加载
     * 组合人工强推产品和无算法推荐结果
     * @param sqlContext
     * @param isCreate
     * @param isCache
     */
    public static void loadRecomendWithoutAlgoForceResult(HiveContext sqlContext,boolean isCreate,boolean isCache,String forceResult) {
        DataFrame df=null;
        //write to parquet
        String path =  TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_FORCERESULT_PATH);
        String sysdt = GetSysDate(0);
        path = path + "-" + sysdt;
        if(isCreate){
            RecomengWithoutAlgo rwa=new RecomengWithoutAlgo();
            //加载得分和推荐结果 RecommendWithoutAlgoResult
            DataFrame df0=App.loadRecommendWithoutAlgoResult(sqlContext, false, false);
            df=rwa.supplementForceInfo(sqlContext,df0,forceResult);
            if(df!=null){
                rwa.saveAsParquet(df,path);
//                System.out.println("df-count():"+df.count());
            }else{
                System.out.println("df = null");
            }
        }else{
            df = sqlContext.load(path);
            df.registerTempTable("RecomendSupplementForceInfo");
            if(isCache)
                sqlContext.cacheTable("RecomendSupplementForceInfo");
        }
    }

    /**
     * 生成或者加载 无算法推荐监控详情表
     * @param sqlContext
     * @param isCreate
     * @param isCache
     */
    public static void loadRecomendWithoutAlgoEvaluateDetail(HiveContext sqlContext,boolean isCreate,boolean isCache) {
        DataFrame df=null;
        //write to parquet
        String path =  TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_EVALUATE_DETAIL_PATH);
        String sysdt = GetSysDate(0);
        path = path + "-" + sysdt;
        System.out.println("path:"+path);
        if(isCreate){
//            RecommendWithoutAlgoEvaluate recommendWithoutAlgoEvaluate=new RecommendWithoutAlgoEvaluate();
             RecommendWithoutAlgoEvaluate_new2 recommendWithoutAlgoEvaluate=new RecommendWithoutAlgoEvaluate_new2();
            df=recommendWithoutAlgoEvaluate.getRecomendWithoutAlgoEvalDetail(sqlContext);
            if(df!=null){
                recommendWithoutAlgoEvaluate.saveAsParquet(df,path);
                String rowkeyStr= TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_TABLE_RECOMEND_NONALGORITHM_EVAL_DETAIL);
                updateHbase(rowkeyStr, sysdt);
                String rowkeyNonStr= TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_TABLE_RECOMEND_NONOWNER_EVAL_DETAIL);
                updateHbase(rowkeyNonStr, sysdt);
            }else{
                System.out.println("df = null");
            }
        }else{
            df = sqlContext.load(path);
            df.registerTempTable("RecomendWithoutAlgoEvaluateDetail");
            if(isCache)
                sqlContext.cacheTable("RecomendWithoutAlgoEvaluateDetail");
        }
    }

    /**
     * 生成或者加载
     * 无算法推荐结果评估
     * @param sqlContext
     * @param isCreate
     * @param isCache
     */
    public static void loadRecomendWithoutAlgoEvaluate(HiveContext sqlContext,boolean isCreate,boolean isCache) {
        DataFrame df=null;
        //write to parquet
        String path =  TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_EVALUATE_PATH);
        String sysdt = GetSysDate(0);
        path = path + "-" + sysdt;
        System.out.println("path:"+path);
        if(isCreate){
            RecommendWithoutAlgoEvaluate recommendWithoutAlgoEvaluate=new RecommendWithoutAlgoEvaluate();
            df=recommendWithoutAlgoEvaluate.MergeResult(sqlContext);
            if(df!=null){
                recommendWithoutAlgoEvaluate.saveAsParquet(df,path);
                String rowkeyStr= TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_TABLE_RECOMEND_NONALGORITHM_EVAL);
                updateHbase(rowkeyStr, sysdt);
            }else{
                System.out.println("df = null");
            }
        }else{
            df = sqlContext.load(path);
            df.registerTempTable("RecomendWithoutAlgoEvaluate");
            if(isCache)
                sqlContext.cacheTable("RecomendWithoutAlgoEvaluate");
        }
    }

    
    
	//Repartition
	public static void parquetRepartition(HiveContext sqlContext, String path, byte[] rowkey){
		if (TK_DataFormatConvertUtil.isExistsPath(path)) {
			
			//重新分区
			Integer repartition_count = 200;
			try {
				repartition_count = Integer.valueOf(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_REPARTITION_COUNT));
			} catch (Exception e) {
				repartition_count = 200;
			}

			//保存为新的临时Parquet
			String tmpPath = path + "-temp";
			TK_DataFormatConvertUtil.deletePath(tmpPath);
			sqlContext.load(path).repartition(repartition_count).saveAsParquetFile(tmpPath);
			
			//将原路径替换为新Parquet
			TK_DataFormatConvertUtil.deletePath(path);
			TK_DataFormatConvertUtil.renamePath(tmpPath, path);
		
			//每天生成时间戳Parquet
			String sysdt = GetSysDate(0);
			String sysdtPath = path + "-" + sysdt;
			TK_DataFormatConvertUtil.deletePath(sysdtPath);
			sqlContext.load(path).saveAsParquetFile(sysdtPath);
			
			//HBASE UPDATE
			String tableName=TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FACTNETTELERESULTINFO);
			byte[] family=Bytes.toBytes(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY)); 
			byte[] qualifier=Bytes.toBytes(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_QUALIFIER)); 
			byte[] value=Bytes.toBytes(sysdt);
			HBaseUtil.put(tableName,rowkey,family,qualifier,value);
		}
	}
	
    
	public static void DeleteCachetable() {
        //delete cachetable 
        String tmpTerminalMapPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FTERMINALMAP_OUTPUTPATH) + "_all";
        TK_DataFormatConvertUtil.deletePath(tmpTerminalMapPath);
        String tmpFDPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_CACHETABLE_PARQUET_PATH) + "/" + FD_TABLE_NAME;
        TK_DataFormatConvertUtil.deletePath(tmpFDPath);
        String tmpCVDPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_CACHETABLE_PARQUET_PATH) + "/" + CVD_TABLE_NAME;
        TK_DataFormatConvertUtil.deletePath(tmpCVDPath);
        String tmpCVIPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_CACHETABLE_PARQUET_PATH) + "/" + CVI_TABLE_NAME;
        TK_DataFormatConvertUtil.deletePath(tmpCVIPath);
    }
    
//    public static String GetSysDate() {
//		Date nowDate = new Date();
//		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
//		String dateStr = dateFormat.format(nowDate);
//		return dateStr;
//	}
    
	public static String GetSysDate(int day) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, day);
		String dateStr = new SimpleDateFormat("yyyyMMdd").format(cal.getTime());
		return dateStr;
	}

	public static void loadFactUserBehaviorIndex(HiveContext sqlContext, boolean isCreate,boolean isCache) {
		String sysdt = GetSysDate(0);
		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORINDEX_OUTPUTPATH);
		path = path + "-" + sysdt;
		DataFrame df = null;
		if(isCreate){
			FactUserBehaviorIndex uBI = new FactUserBehaviorIndex();
			df = uBI.getFactUserBehaviorIndex(sqlContext);
			uBI.saveAsParquet(df, path);
			String rowkeyStr = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FACTUSERBEHAVIORINDEX_ROWKEY);
			updateHbase(rowkeyStr,sysdt);
		}else{
			df = sqlContext.load(path);
			df.registerTempTable(FACT_USERBEHAVIOR_INDEX);
			if(isCache)
				sqlContext.cacheTable(FACT_USERBEHAVIOR_INDEX);
 		}
	}
	
	public static void updateHbase(String rowkeyStr,String date){
		//HBASE UPDATE
		String tableName=TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FACTNETTELERESULTINFO);
		byte[] rowkey=Bytes.toBytes(rowkeyStr);
		byte[] family=Bytes.toBytes(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY)); 
		byte[] qualifier=Bytes.toBytes(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_QUALIFIER)); 
		byte[] value=Bytes.toBytes(date);
		HBaseUtil.put(tableName,rowkey,family,qualifier,value);
	}

	public static void loadUbaLogEvent(HiveContext sqlContext,String tableName,boolean isCache) {
		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_UBALOGEVENT_OUTPUTPATH);
		DataFrame df = sqlContext.load(path);
		df.registerTempTable(tableName);
		if(isCache)
			sqlContext.cacheTable(tableName);
	}
	
	
	public static DataFrame loadParquet(HiveContext sqlContext,String path,String tableName,boolean isCache){
		DataFrame df = sqlContext.load(path);
		df.registerTempTable(tableName);
		if(isCache)
			sqlContext.cacheTable(tableName);
		return df;
	}
	
	/**
	 * 
	 * @Description: repation通用实现
	 * @param sqlContext
	 * @param path
	 * @author moyunqing
	 * @date 2016年9月13日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	public static void repatition(HiveContext sqlContext,String path){
		String tmpPath = path + "-temp";
		final Integer DEFAULT_PATION = 200;
		Integer pation = 0; 
		try {
			pation = Integer.valueOf(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_REPARTITION_COUNT));
			if(pation <= 0)
				pation = DEFAULT_PATION;
		} catch (Exception e) {
			pation = DEFAULT_PATION;
		}
		//Load parquet[path], save as parquet[tmpPath]
		TK_DataFormatConvertUtil.deletePath(tmpPath);
		sqlContext.load(path).repartition(pation).saveAsParquetFile(tmpPath);
		//Delete parquet[path]
		TK_DataFormatConvertUtil.deletePath(path);
		//Rename parquet[tmpPath] as parquet[path]
		TK_DataFormatConvertUtil.renamePath(tmpPath, path);
	}

	/**
	 * 
	 * @Description: 更新线索类型字段
	 * @param sqlContext
	 * @param path
	 * @param isAll
	 * @param target
	 * @author moyunqing
	 * @date 2016年9月20日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	public static void updateUBClueType(HiveContext sqlContext, String path, String isAll, String target) {
		if(StringUtils.isNotBlank(path)){
			String tableName = "Tmp_ubh";
			BehaviorCommon bcomm = new BehaviorCommon();
			loadParquet(sqlContext, path, tableName, false);
			DataFrame df = bcomm.getUBHClue(sqlContext, tableName, isAll, target);
			bcomm.saveAsParquet(sqlContext,df,tableName,path,isAll);
		}
	}
    
	/**
	 * 
	 * @Description: 加载通用投保流程中间表信息
	 * @param sqlContext
	 * @param pro
	 * @author moyunqing
	 * @date 2016年9月24日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	public static void loadInsuranceProcessEvent(HiveContext sqlContext, Properties pro) {
		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTINSURANCEPROCESSEVENT_OUTPUTPATH);
		InsuranceProcessEvent proEvent = new InsuranceProcessEvent();
		Map<String,Map<String,String>> config = new HashMap<String,Map<String,String>>();
		String isallStr = pro.getProperty("insurance.process.clue.event.isall");
		boolean isall = false;
		if(StringUtils.isNotBlank(isallStr) && Boolean.parseBoolean(isallStr))
			isall = true;
		String types = pro.getProperty("insurance.process.clue.type");
		if(StringUtils.isNotBlank(types)){
			Map<String,String> attr = null;
			String [] typeArr = types.split(",");
			String sign = "";
			String lrtid = "";
			for(String type : typeArr){
				sign = pro.get(type + ".sign") == null?"":pro.get(type + ".sign").toString();
				lrtid = pro.get(type + ".lrtid") == null?"":pro.get(type + ".lrtid").toString();
				if(StringUtils.isNotBlank(sign) && StringUtils.isNotBlank(lrtid)){
					attr = new HashMap<String,String>();
					attr.put("sign", sign);
					attr.put("lrtid", lrtid);
				}
				config.put(type, attr);
			}
		}
		DataFrame df = proEvent.getInsuranceProcess(sqlContext, config,isall);
		proEvent.save(df,path,isall);
		if(!isall)
			repatition(sqlContext, path);
	}

	/**
	 * 
	 * @Description: 加载通用投保流程线索表
	 * @param sqlContext
	 * @param pro
	 * @author moyunqing
	 * @date 2016年9月29日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	public static void loadInsuranceProcessClue(HiveContext sqlContext, Properties pro) {
		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_WH_FACTUSERBEHAVIORCLUE_OUTPUTPATH);
		InsuranceProcessClue proClue = new InsuranceProcessClue();
		Map<String,String> config = new HashMap<String,String>();
		String isallStr = pro.getProperty("insurance.process.clue.isall");
		boolean isall = false;
		if(StringUtils.isNotBlank(isallStr) && isallStr.equals("true"))
			isall = true;
		config.put("failedType",pro.get("isclue.failed.clue.type") == null?"":pro.get("isclue.failed.clue.type").toString());
		config.put("successType",pro.get("isclue.success.clue.type") == null?"":pro.get("isclue.success.clue.type").toString());
		config.put("failedLrtId",pro.get("isclue.failed.lrtid") == null?"":pro.get("isclue.failed.lrtid").toString());
		config.put("successLrtId",pro.get("isclue.success.lrtid") == null?"":pro.get("isclue.success.lrtid").toString());
		DataFrame df = proClue.getInsuranceProcessClue(sqlContext, config,isall);
		proClue.save(df,path,isall);
		if(!isall)
			repatition(sqlContext, path);
//		String toDay = GetSysDate(0);
//		df = sqlContext.load(path);
//		path = path + "-" + toDay;
//		proClue.save(df,path,true);
//		String rowkey = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_WH_FACT_USERBEHAVIOR_CLUE_ROWKEY);
//		updateHbase(rowkey, toDay);
	}
	
	//AccountWorhtSearch
    public static void loadFactUserBehaviorClueMobileAccountWorthSearch(HiveContext sqlContext,String appids) {
    	MobileAccountWorthSearchService mobileAccountWorthSearchService = new MobileAccountWorthSearchService();
    	DataFrame userBehaviorClueDF = mobileAccountWorthSearchService.getUserBehaviorClueDF(sqlContext, appids);
    	
    	if(userBehaviorClueDF != null){
    		JavaRDD<FactUserBehaviorClue> userBehaviorClueRDD = mobileAccountWorthSearchService.getJavaRDD(userBehaviorClueDF);
    		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORCLUE_OUTPUTPATH);
    		mobileAccountWorthSearchService.pairRDD2Parquet(sqlContext, userBehaviorClueRDD, path);
    		repatition(sqlContext, path);
    	}
    }
    
    //Wap_service_page
    public static void loadFactUserBehaviorClueWapServicePage(HiveContext sqlContext,String appids) {
    	MobileAccountWorthSearchService mobileAccountWorthSearchService = new MobileAccountWorthSearchService();
    	DataFrame userBehaviorClueDF = mobileAccountWorthSearchService.getUserBehaviorClueDF(sqlContext, appids);
    	
    	if(userBehaviorClueDF != null){
    		JavaRDD<FactUserBehaviorClue> userBehaviorClueRDD = mobileAccountWorthSearchService.getJavaRDD(userBehaviorClueDF);
    		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORCLUE_OUTPUTPATH);
    		mobileAccountWorthSearchService.pairRDD2Parquet(sqlContext, userBehaviorClueRDD, path);
    		repatition(sqlContext, path);
    	}
    }
    //Health Service
    public static void loadFactUserBehaviorClueHealthService(HiveContext sqlContext,String appids) {
    	HealthPageService healthPageService = new HealthPageService();
    	DataFrame userBehaviorClueDF = healthPageService.getUserBehaviorClueDF(sqlContext, appids);
    	
    	if(userBehaviorClueDF != null){
    		JavaRDD<FactUserBehaviorClue> userBehaviorClueRDD = healthPageService.getJavaRDD(userBehaviorClueDF);
    		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORCLUE_OUTPUTPATH);
    		healthPageService.pairRDD2Parquet(sqlContext, userBehaviorClueRDD, path);
    		repatition(sqlContext, path);
    	}
    }
    public static void loadSpringFestivalPurchasesClue(HiveContext sqlContext,String appids) {
        SpringFestivalPurchaseClueImpl wacImpl=new SpringFestivalPurchaseClueImpl();
        WechatActivityClueCommon sfp=new WechatActivityClueCommon(wacImpl);

        DataFrame userBehaviorClueDF = sfp.getWechatActivityClue(sqlContext, appids);

        if(userBehaviorClueDF != null){
            JavaRDD<FactUserBehaviorClue> userBehaviorClueRDD = sfp.getJavaRDD(userBehaviorClueDF);
            String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORCLUE_OUTPUTPATH);
//            String path = "/user/tkonline/taikangtrack_test/data/fact_user_behavior_clue_sf";
            sfp.pairRDD2Parquet(sqlContext, userBehaviorClueRDD, path);
            repatition(sqlContext, path);
        }
    }
    
    //H5_insure_flow 商品详情页
    public static void loadFactUserBehaviorFlowGoodsDetail(HiveContext sqlContext,String appids) {
    	UserBehaviorFlowGoodsDetail gd = new UserBehaviorFlowGoodsDetail();
        DataFrame gdDf = gd.getUserBehaviorFlowGoodsDetailDF(sqlContext, appids);

        if(gdDf!=null){
        	String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORTELE_OUTPUTPATH);
        	gdDf.save(path, "parquet", SaveMode.Append);
	        repatition(sqlContext, path);
        }
    }
    

	public static void loadEStationUserbehaviorData(HiveContext sqlContext) {
		try {
			EStation2HomeWeb EW = new EStation2HomeWeb();
			JavaRDD<FactUserBehaviorClue> pRDD =EW.getEStation2HomeWeb(sqlContext);
			String saveDataPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_WH_FACTUSERBEHAVIORCLUE_OUTPUTPATH);
			EW.pairRDD2Parquet(sqlContext, pRDD, saveDataPath);
			App.repatition(sqlContext, saveDataPath);//重分区
//			String toDay = DateTimeUtil.getSysDate(0);
//			DataFrame df = sqlContext.load(saveDataPath);
//			saveDataPath = saveDataPath + "-" + toDay;
//			df.save(saveDataPath, "parquet", SaveMode.Append);
//			String rowkey = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_WH_FACT_USERBEHAVIOR_CLUE_ROWKEY);
//			updateHbase(rowkey, toDay);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	public static void loadUserBehaviorBrowseCar(HiveContext sqlContext,String appids){
		UserBehaviorBrowseCar  mUserBehaviorClue=new UserBehaviorBrowseCar();
		DataFrame UserBehaviorTeleDF =mUserBehaviorClue.getUserBehaviorCarCuleDF(sqlContext, appids);
		if(UserBehaviorTeleDF!=null){
			String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORCLUE_OUTPUTPATH);
			UserBehaviorTeleDF.save(path, "parquet", SaveMode.Append);
        }
	}

    public static void loadUserBehaviorActivityAcciMonth4(HiveContext sqlContext, String appids){
        UserBehaviorActivitiesAccMonth4 uba4month = new UserBehaviorActivitiesAccMonth4();
        DataFrame UserBehaviorActivityAcciDF =uba4month.getUserBehaviorActivitiesAcciCuleDF(sqlContext, appids);
        if(UserBehaviorActivityAcciDF!=null){
        	String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTACTIVITIES_MONTH4_ACCI_OUTPUTPATH);
            UserBehaviorActivityAcciDF.save(path, "parquet", SaveMode.Append);
        }
    }
    public static void loadUserBehaviorAcciMonth4LoveContract(HiveContext sqlContext, String appids){
        UserBehaviorActivitiesAccMonth4 uba4month = new UserBehaviorActivitiesAccMonth4();
        DataFrame UserBehaviorActivityAcciDF =uba4month.getUserBehaviorActivitiesAcciCuleDF(sqlContext, appids);
        if(UserBehaviorActivityAcciDF!=null){
        	String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTACTIVITIES_MONTH4_LOVECONTRACT_OUTPUTPATH);
            UserBehaviorActivityAcciDF.save(path, "parquet", SaveMode.Append);
        }
    }  
    public static void loadUserBehaviorActivityMonth6(HiveContext sqlContext, String appids){
    	UserBehaviorActivitiesMonth6 uba6month = new UserBehaviorActivitiesMonth6();
    	DataFrame userBehaviorActivityDF = uba6month.getUserBehaviorActivitiesCuleDF(sqlContext, appids);
    	if(userBehaviorActivityDF != null){
    		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTACTIVITIES_MONTH6_OUTPUTPATH);
    		userBehaviorActivityDF.save(path, "parquet", SaveMode.Append);
    	}
    }
	
    /**
     * 渠道客户转化微信端
     * 
     * @param sqlContext
     * @param appids
     */
    public static void loadChannelCustomerConversion(HiveContext sqlContext, String appids){
        ChannelCustomerConversion chan = new ChannelCustomerConversion();
        DataFrame cCustomerConversionCule =chan.getChannelCustomerConversionCuleDF(sqlContext, appids);
        if(cCustomerConversionCule!=null){
            String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORCLUE_OUTPUTPATH);
            cCustomerConversionCule.save(path, "parquet", SaveMode.Append);
        }
    }
    /**
     * 微信端计算保费
     * @param sqlContext
     * @param appids
     */
    public static void loadUserBehaviorCountPremium(HiveContext sqlContext,String appids){
//		UserBehaviorCountPremium userBehaviorCountPremium = new UserBehaviorCountPremium();
//		DataFrame UserBehaviorCountPremiumDF =userBehaviorCountPremium.getUserBehaviorCarCuleDF(sqlContext, appids);
//		if(UserBehaviorCountPremiumDF!=null){
//			String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORCLUE_OUTPUTPATH);
//			UserBehaviorCountPremiumDF.save(path, "parquet", SaveMode.Append);
//        }
		UserBehaviorCountPremium userBehaviorCountPremium = new UserBehaviorCountPremium();
		DataFrame UserBehaviorCountPremiumDF =userBehaviorCountPremium.getUserBehaviorCarCuleDF(sqlContext, appids);
		if(UserBehaviorCountPremiumDF!=null){
			JavaRDD<FactUserBehaviorClue> userBehaviorClueRDD =userBehaviorCountPremium.getJavaRDD(UserBehaviorCountPremiumDF);
			String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORCLUE_OUTPUTPATH);
			userBehaviorCountPremium.pairRDD2Parquet(sqlContext, userBehaviorClueRDD, path);	
			repatition(sqlContext, path);
        }
	}
    
    /**
     * 微信端免费体检
     * 
     * @param sqlContext
     * @param appids
     */
    public static void loadWchatFreeHealthCheck(HiveContext sqlContext, String appids){
    	UserBehaviorWechatFreeCheck wfc = new UserBehaviorWechatFreeCheck();
        DataFrame freeCheckCule =wfc.getWechatFreeCheckCuleDF(sqlContext, appids);
        if(freeCheckCule!=null){
            String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_WEFREECHECKBEHAVIORCLUE_OUTPUTPATH);
            freeCheckCule.save(path, "parquet", SaveMode.Append);
        }
    }
    
    /**
     * 坐席电商APP使用情况分析
     * 
     * @param sqlContext
     * @param appids
     */
    public static void loadTeleAppUseSummary(HiveContext sqlContext){

    	TeleAppUseSummary tUserCondition=new TeleAppUseSummary();
        SrcUserEvent mSrcUserEvent = new SrcUserEvent();
        // /user/tkonline/taikangtrack/data/uba_log_event
    	String FILENAME_WRITE = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_UBALOGEVENT_OUTPUTPATH);
    	//拼上路径和昨天的时间
    	String dailyPath = mSrcUserEvent.getDailyPath(FILENAME_WRITE);
    	//把 uba_log_event 注册到大数据平台的临时表
		sqlContext.load(dailyPath).registerTempTable("TMP_TELEAPPUSESUMMARY");		
       
		DataFrame StatisticsEventDF = tUserCondition.getTeleAppUseSummaryDF(sqlContext);
		if(StatisticsEventDF != null){   
       	JavaRDD<TempTeleAppUseSummary> teleUseConditionRDD = tUserCondition.getJavaRDD(StatisticsEventDF);
       	String TELEUSECONDITION_OUTPUTPATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_TELEAPPUSESUMMARY_OUTPUTPATH);
       	tUserCondition.pairRDD2Parquet(sqlContext, teleUseConditionRDD, TELEUSECONDITION_OUTPUTPATH);       
       	}
		//更新hbase 中的currentdate
       	String todaydate = TimeUtil.getNowStr("yyyy-MM-dd");
		todaydate = todaydate+" 00:00:00";
       	BaseTable.updateHbase(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_TABLE_TELE_APP_USE_SUMMARY), todaydate);
		
    }
    
    /**
     * 营销短信统计
     * 
     */
    public static void loadSmsScodeStatistic(HiveContext sqlContext){
    	
    	UserSmsBehavior userSmsBehavior = new UserSmsBehavior();
    	String FILENAME_WRITE = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_UBALOGEVENT_OUTPUTPATH);
    	//把 uba_log_event 注册到大数据平台的临时表
    	sqlContext.load(FILENAME_WRITE).registerTempTable("UBA_LOG_EVENT");		
    	String USERSMSBEHAVIOR_OUTPUTPATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_USERSMSBEHAVIOR_OUTPUTPATH);
    	if (TK_DataFormatConvertUtil.isExistsPath(USERSMSBEHAVIOR_OUTPUTPATH)) {
    		TK_DataFormatConvertUtil.deletePath(USERSMSBEHAVIOR_OUTPUTPATH);
    	}
    	List<UserSmsConfig> userSmsConfigs = userSmsBehavior.getUserSmsConfigs();
    	for(UserSmsConfig userSmsConfig : userSmsConfigs) {
    		DataFrame userSmsBehaviorDF = userSmsBehavior.getUserSmsBehaviorDF(sqlContext, userSmsConfig);
    		if(userSmsBehaviorDF != null){ 
    			JavaRDD<TempUserSmsBehavior> userSmsBehaviorRDD = userSmsBehavior.getJavaRDD(userSmsBehaviorDF);
    			userSmsBehavior.pairRDD2Parquet(sqlContext, userSmsBehaviorRDD, USERSMSBEHAVIOR_OUTPUTPATH);       
    		}
    	}
    	//更新hbase 中的currentdate
    	String todaydate = TimeUtil.getNowStr("yyyy-MM-dd");
    	todaydate = todaydate+" 00:00:00";
    	BaseTable.updateHbase(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_USERSMSBEHAVIOR_ROWKEY), todaydate);
    	
    }
    
    public static void loadCommonUserBehaviorClue(HiveContext sqlContext) {
		UserBehaviorClueCommon userBehaviorClueCommon = new UserBehaviorClueCommon();
    	List<UserBehaviorConfigVo> userBehaviorConfigVos = userBehaviorClueCommon.getUserBehaviorConfigVos();
		for(UserBehaviorConfigVo userBehaviorConfigVo : userBehaviorConfigVos) {
			DataFrame userBehaviorClueDF = userBehaviorClueCommon.getUserBehaviorClueDF(sqlContext, userBehaviorConfigVo);
			if(userBehaviorClueDF != null){
				String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_COMMON_CLUE_OUTPUTPATH);
				userBehaviorClueDF.save(path, "parquet", SaveMode.Append);
			}
		}
	}
    
    public static void loadTeleShareSummary(HiveContext sqlContext){

    	TeleShareSummary tUserCondition=new TeleShareSummary();
        SrcUserEvent mSrcUserEvent = new SrcUserEvent();
        // /user/tkonline/taikangtrack/data/uba_log_event
    	String FILENAME_WRITE = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_UBALOGEVENT_OUTPUTPATH);
    	//拼上路径和昨天的时间
    	String dailyPath = mSrcUserEvent.getDailyPath(FILENAME_WRITE);
    	//把 uba_log_event 注册到大数据平台的临时表
		sqlContext.load(dailyPath).registerTempTable("TMP_TELESHARESUMMARY");		
       
		DataFrame StatisticsEventDF = tUserCondition.getTeleShareSummaryDF(sqlContext);
		if(StatisticsEventDF != null){   
       	JavaRDD<TempTeleShareSummary> teleUseConditionRDD = tUserCondition.getJavaRDD(StatisticsEventDF);
       	String TELEUSECONDITION_OUTPUTPATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_TELEPSHARESUMMARY_OUTPUTPATH);
       	tUserCondition.pairRDD2Parquet(sqlContext, teleUseConditionRDD, TELEUSECONDITION_OUTPUTPATH);
       	}       	
		System.out.println("开始更新当前日期 ：");
       	//更新hbase 中的currentdate
       	String todaydate = TimeUtil.getNowStr("yyyy-MM-dd");
		todaydate = todaydate+" 00:00:00";
       	BaseTable.updateHbase(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_TABLE_TELE_SHARE_SUMMARY), todaydate);
		
    }
    /**
     * 微信七月满送活动
     * @param sqlContext
     * @param appids
     */
    public static void loadUserBehaviorJulyActivity(HiveContext sqlContext,String appids){
    	UserBehaviorJulyActivity userBehaviorJulyActivity = new UserBehaviorJulyActivity();
		DataFrame userBehaviorCarCuleDF = userBehaviorJulyActivity.getUserBehaviorCarCuleDF(sqlContext, appids);
		if(userBehaviorJulyActivity!=null){
			JavaRDD<FactUserBehaviorClue> userBehaviorClueRDD =userBehaviorJulyActivity.getJavaRDD(userBehaviorCarCuleDF);
			String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_JULYACTIVITYCLUE_OUTPUTPATH);
			userBehaviorJulyActivity.pairRDD2Parquet(sqlContext, userBehaviorClueRDD, path);	
			repatition(sqlContext, path);
        }
	}
    
    /**
     * 健康告知页面停留时长统计
     * @param sqlContext
     * @param isCreate
     * @param isCreate
     */
    public static void loadHealthClauseDuration(HiveContext sqlContext,boolean isCreate,boolean isCache){
    	String path =  TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_HEALTHDURATION_PATH)+"-"+GetSysDate(0);
        HealthClauseDuration healthClauseDuration = new HealthClauseDuration();
    	DataFrame dataFrame =healthClauseDuration.getHealthDuration(sqlContext);
		if (dataFrame != null) {
			healthClauseDuration.saveAsParquet(dataFrame, path);
			String rowkeyStr = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_TABLE_HEALTHDURATION_ROWKEY);
			updateHbase(rowkeyStr, GetSysDate(0));
		} else {
			System.out.println("*************HealthDurationDataFrame is null*************");
		}
    }
}