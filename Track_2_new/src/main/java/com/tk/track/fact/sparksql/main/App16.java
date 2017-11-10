package com.tk.track.fact.sparksql.main;

import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;
import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;

public class App16 {
	
	private static final String CONFIG_PATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_TRACK_CONFIG_PATH_ROOT) + "/cluetype-config.properties";
	
	public static void main(String[] args) {
		 SparkConf conf = new SparkConf().setAppName("Taikang Track InsuranceProcessClue ETL APP16 ");
		 conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, 
				 TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
		 SparkContext sc = new SparkContext(conf);
		 HiveContext sqlContext = new HiveContext(sc);
		 RandomUDF.genrandom(sc, sqlContext);  
		 RandomUDF.genUniq(sc, sqlContext);
		 RandomUDF.spequals(sc, sqlContext);
		 String insrcprcstPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTINSURANCEPROCESSEVENT_OUTPUTPATH);
		 //加载FACT_INSURANCEPROCESS_EVENT 
		 App.loadParquet(sqlContext, insrcprcstPath, "FACT_INSURANCEPROCESS_EVENT", false);
		 //加载FACT_USERINFO
		 String factUserInfoPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_USERINFO_INPUTPATH);
		 App.loadParquet(sqlContext, factUserInfoPath, "FACT_USERINFO", false);
		 //加载P_LIFEINSURE
		 String pLifeinsurePath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_PLIFEINSURE_PARQUET);	        //加载P_LIFEINSURE
		 App.loadParquet(sqlContext, pLifeinsurePath, "P_LIFEINSURE", false);
		 
		 Properties pro = TK_CommonConfig.getConfig(CONFIG_PATH);
		 App.loadInsuranceProcessClue(sqlContext,pro);
		 sqlContext.clearCache();
		 sc.stop();
	}
}
