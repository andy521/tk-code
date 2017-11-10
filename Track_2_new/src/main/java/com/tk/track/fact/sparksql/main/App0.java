package com.tk.track.fact.sparksql.main;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;

public class App0 {
	private static final String CONFIG_PATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_TRACK_CONFIG_PATH_ROOT) + "/appid-type.properties";
	
	public static void main(String[] args) {
		 SparkConf conf = new SparkConf().setAppName("Taikang Track BehaviorCommon ETL APP0");
	        conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, 
	                TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
	        SparkContext sc = new SparkContext(conf);
	        HiveContext sqlContext = new HiveContext(sc);
	        RandomUDF.genrandom(sc, sqlContext);        
	        String behaviorTelePath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORTELE_OUTPUTPATH);
	        String isAll = TK_CommonConfig.getConfigValue(CONFIG_PATH, "clue.type.isall");
	        String target = TK_CommonConfig.getConfigValue(CONFIG_PATH, "clue.type.target");
	        //update clue_type for behavior table 
	        App.updateUBClueType(sqlContext,behaviorTelePath,isAll,target);
	        
	        //Repartition
	    	byte[] behaviorTeleRowkey=Bytes.toBytes(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FACTUSERBEHAVIORTELE_ROWKEY));
	        App.parquetRepartition(sqlContext, behaviorTelePath, behaviorTeleRowkey);
	        
	        //原来app10
	        String factUserInfoPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_USERINFO_INPUTPATH);
	        App.loadParquet(sqlContext, factUserInfoPath, "FACT_USERINFO", false);
	        //UserPolicyInfo
	    	RandomUDF.getUUID(sc, sqlContext);
	        App.loadUserPolicyInfo(sqlContext);
	        
	        sqlContext.clearCache();
			sc.stop();
	}

}
