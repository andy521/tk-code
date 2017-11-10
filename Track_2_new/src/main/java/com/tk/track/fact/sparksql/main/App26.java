package com.tk.track.fact.sparksql.main;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by t-chenhao01 on 2016/11/25.
 */
public class App26 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Taikang Track recommend without algorithm ETL APP26");
        conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
        SparkContext sc = new SparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);
        RandomUDF.genrandom(sc, sqlContext);
//        RandomUDF.gendecode(sc, sqlContext);

        //加载推荐基准表 Recommend_Without_Algo_Base
//        App.loadRecommendWithoutAlgoBase(sqlContext,false,false);

        App.loadUbaLogEvent(sqlContext, "UBA_LOG_EVENT", false);
        
        String terminalMapPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FTERMINALMAP_OUTPUTPATH);
		App.loadParquet(sqlContext, terminalMapPath, "f_terminal_map", false);
        //加载大健康表
        String facthealthcreditscoreresultPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTHEALTHCREDITSCORERESULT_INPUTPATH);
        facthealthcreditscoreresultPath=App23.getDailyPath(facthealthcreditscoreresultPath, "_", "yyyy-MM-dd", -0);
        App.loadParquet(sqlContext, facthealthcreditscoreresultPath, "fact_healthcreditscoreresult", false);
      
        String sysdt = GetSysDate();
        String policy_summary=TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FNETPOLICYSUMMARY_OUTPUTPATH);
        String policysummaryPath = policy_summary + "-" + sysdt;
        App.loadParquet(sqlContext, policysummaryPath, "f_net_policysummary", false);

        App.loadParquet(sqlContext, "/user/tkonline/common-parquet/GUPOLICYCOPYMAIN", "GUPOLICYCOPYMAIN", false);
        App.loadParquet(sqlContext, "/user/tkonline/common-parquet/GUPOLICYRELATEDPARTY", "GUPOLICYRELATEDPARTY", false);

        App.loadRecomendWithoutAlgoEvaluateDetail(sqlContext, true,false);
        App.loadRecomendWithoutAlgoEvaluate(sqlContext, true, false);


        //删除三天前结果数据
        String path =  TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_TABLE_RECOMEND_NONALGORITHM_EVAL_DETAIL);
        App23.delete3daysAgoFile(path, "-", "yyyyMMdd");
        String path2 =  TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_EVALUATE_PATH);
        App23.delete3daysAgoFile(path2, "-", "yyyyMMdd");
        String pathNonOwner =  TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDNONOWNER_EVALUATE_DETAIL_PATH);
        App23.delete3daysAgoFile(pathNonOwner, "-", "yyyyMMdd");

        sqlContext.clearCache();
        sc.stop();
    }
    
    public static String GetSysDate() {
		Date nowDate = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		String dateStr = dateFormat.format(nowDate);
		return dateStr;
	}
	

}
