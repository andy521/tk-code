package com.tk.track.fact.sparksql.main;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;
import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;

public class App15 {
	
	private static final String CONFIG_PATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_TRACK_CONFIG_PATH_ROOT) + "/cluetype-config.properties";
	
	public static void main(String[] args) {
		 SparkConf conf = new SparkConf().setAppName("Taikang Track InsuranceProcessEvent ETL APP15 ");
	        conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, 
	                TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
	        SparkContext sc = new SparkContext(conf);
	        HiveContext sqlContext = new HiveContext(sc);
	        RandomUDF.genrandom(sc, sqlContext);  
	        RandomUDF.genUniq(sc, sqlContext);
	        RandomUDF.spequals(sc, sqlContext);
	        String ubaPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_UBALOGEVENT_OUTPUTPATH);
	        //加载uba_log_event
	        App.loadParquet(sqlContext, ubaPath, "Tmp_uba", false);
	        String applyPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_PAPPLYFORM_PARQUET);
	        //加载订单表
	        App.loadParquet(sqlContext, applyPath, "P_APPLYFORM", false);
	        String terminalMapPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FTERMINALMAP_OUTPUTPATH);
	        //加载设备映射表
	        App.loadParquet(sqlContext, terminalMapPath, "Fact_TerminalMap", true);
	        
	        Properties pro = TK_CommonConfig.getConfig(CONFIG_PATH);
	        App.loadInsuranceProcessEvent(sqlContext,pro);
	        sqlContext.clearCache();
			sc.stop();
	}
}
