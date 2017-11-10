package com.tk.track.fact.sparksql.main;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;

/**
 * @author itw_shayl 健康告知页面停留时常统计 2017-7-18
 *
 */
public class App45 {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Taikang Track Health Clause Duration ETL App45");
		conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
		SparkContext sc = new SparkContext(conf);
		HiveContext sqlContext = new HiveContext(sc);
		RandomUDF.getUUID(sc, sqlContext);
		RandomUDF.genrandom(sc, sqlContext);
		
		//加载:UBA_LOG_EVENT(用户行为表)
		App.loadUbaLogEvent(sqlContext, "UBA_LOG_EVENT", false);
        //加载:fact_healthcreditscoreresult(大健康表)
        String facthealthcreditscoreresultPath =getDailyPath(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTHEALTHCREDITSCORERESULT_INPUTPATH), "_", "yyyy-MM-dd", -1);
        App.loadParquet(sqlContext, facthealthcreditscoreresultPath, "fact_healthcreditscoreresult", false);
        //加载:FACT_USERINFO(用户信息表)
        App.loadParquet(sqlContext, TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_USERINFO_INPUTPATH), "FACT_USERINFO", false);
        //加载:f_net_policysummary(寿险表)
        App.loadParquet(sqlContext, TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FNETPOLICYSUMMARY_OUTPUTPATH), "f_net_policysummary", false);
        //加载:GUPOLICYCOPYMAIN(财险主表)
        App.loadParquet(sqlContext, TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_GUPOLICYCOPYMAIN_PARQUET), "GUPOLICYCOPYMAIN", false);
        //加载:GUPOLICYRELATEDPARTY(财险表)
        App.loadParquet(sqlContext, TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_GUPOLICYRELATEDPARTY_PARQUET), "GUPOLICYRELATEDPARTY", false);
       
        //处理:FinalDuration(健康告知页面停留时长)
        App.loadHealthClauseDuration(sqlContext,true,false);
		
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
	private static String getDailyPath(String path,String connector,String parten,int dayDif) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, dayDif);
        String yesterday = new SimpleDateFormat(parten).format(cal.getTime());
        return (path + connector + yesterday);
    }
}
