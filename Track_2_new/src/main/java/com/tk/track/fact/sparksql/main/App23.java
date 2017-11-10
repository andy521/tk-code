package com.tk.track.fact.sparksql.main;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.etl.RecomengWithoutAlgo;
import com.tk.track.fact.sparksql.udf.RandomUDF;
import com.tk.track.util.TK_DataFormatConvertUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class App23 {
	// 外部配置文件 理赔流程appid
	private static final String CONFIG_PATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_TRACK_CONFIG_PATH_ROOT) + "/appid-type.properties";
	private static final String STATISTICS_EVENT_FILENAME_WRITE = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_ANONY_OUTPUTPATH);

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Taikang Track recommend without algorithm ETL APP23");
//				set("sqark.shuffle.consolidateFiles", "true");;
		conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
		SparkContext sc = new SparkContext(conf);
		HiveContext sqlContext = new HiveContext(sc);
		RandomUDF.genrandom(sc, sqlContext);
//		RandomUDF.gendecode(sc, sqlContext);
//		RandomUDF.contactProductInfo(sc, sqlContext);//注册自定义UDF函数，用来从list2补充list1

		App.loadUbaLogEvent(sqlContext, "UBA_LOG_EVENT", false);
//		App.loadUbaLogEvent(sqlContext, "TMP_UBA_LOG_EVENT", false);//当日的


		String terminalMapPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FTERMINALMAP_OUTPUTPATH);
		App.loadParquet(sqlContext, terminalMapPath, "TERMINAL_MAP_APP_FULL", false);
		//生成匿名用户的统计 (全量)
		App.loadFStatisticsEventWithAnony(sqlContext);

//		//加载匿名用户的统计
//		App.loadParquet(sqlContext, STATISTICS_EVENT_FILENAME_WRITE, "TMP_FACT_STATISTICS_EVENT", false);

		//加载登录用户的统计
		String staticsEventPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH);
		App.loadParquet(sqlContext, staticsEventPath, "F_STATISTICS_EVENT", false);

		String factUserInfoPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_USERINFO_INPUTPATH);
        App.loadParquet(sqlContext, factUserInfoPath, "FACT_USERINFO", false);

        //加载有效保单
        String factNetPolicyInfoResult = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTNETPOLICYINFORESULT_OUTPUTPATH);
        factNetPolicyInfoResult=getDailyPath(factNetPolicyInfoResult,"-","yyyyMMdd",0);
        App.loadParquet(sqlContext, factNetPolicyInfoResult, "FactPInfoResult", false);
        //加载大健康表
        String facthealthcreditscoreresultPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTHEALTHCREDITSCORERESULT_INPUTPATH);
        facthealthcreditscoreresultPath=getDailyPath(facthealthcreditscoreresultPath,"_","yyyy-MM-dd",0);
        App.loadParquet(sqlContext, facthealthcreditscoreresultPath, "fact_healthcreditscoreresult", false);
//    
        //计算推荐基准表
        App.loadRecommendWithoutAlgoBase(sqlContext, true, false);


//		//TODO 单独测试一个流程用
//		RecomengWithoutAlgo recomengWithoutAlgo =new RecomengWithoutAlgo();
//		recomengWithoutAlgo.getFactHealthcreditscoreresultNotNullDF(sqlContext);


		String recommendBasepath =  TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_OUTPUTPATH);
		delete3daysAgoFile(recommendBasepath,"-","yyyyMMdd");

//		//TODO 单独测试一个流程用
//		recommendBasepath=getDailyPath(recommendBasepath,"-","yyyyMMdd",0);
//		App.loadParquet(sqlContext, recommendBasepath, "RBASE_USERBROWSEANDPOLICYINFO", false);


		//计算一段时间内产品浏览排名，用来做默认
		App.loadDefaultRecommendWithoutAlgoResult(sqlContext, true, false);
		String defaultRecommendWithoutAlgoResultpath =  TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGORESULTDEFAULT_OUTPUTPATH);
		delete3daysAgoFile(defaultRecommendWithoutAlgoResultpath,"-","yyyyMMdd");

     
      //计算得分和推荐结果
        App.loadRecommendWithoutAlgoResult(sqlContext, true, false);

	 //删除三天前推荐结果数据
        String recommendWithoutAlgoResultpath =  TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGOSCORE_OUTPUTPATH);
		delete3daysAgoFile(recommendWithoutAlgoResultpath,"-","yyyyMMdd");

		sqlContext.clearCache();
		sc.stop();
	}

	/**
	 * 拿到代日期的路径
	 * @param path		原始路径
	 * @param connector 连接符
	 * @param parten	日期模式
	 * @param dayDif	前推几天
	 * @return
	 */
	public static String getDailyPath(String path,String connector,String parten,int dayDif) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, dayDif);
        String yesterday = new SimpleDateFormat(parten).format(cal.getTime());
        return (path + connector + yesterday);
    }

	/**
	 * 删除三天前的hdfs无用文件
	 * @param orginalPath 原始路径
	 * @param connectStr  与日期的连接符
	 * @param datePattern   日期模式 yyyy MM dd
	 */
	public static void delete3daysAgoFile(String orginalPath,String connectStr,String datePattern){
		String path=getDailyPath(orginalPath, connectStr, datePattern,-3);
		TK_DataFormatConvertUtil.deletePath(path);
	}

}
