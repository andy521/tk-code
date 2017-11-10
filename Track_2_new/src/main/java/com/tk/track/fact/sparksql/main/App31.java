package com.tk.track.fact.sparksql.main;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.udf.RandomUDF;

public class App31 {
	
	private static final String CONFIG_PATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_TRACK_CONFIG_PATH_ROOT) + "/appid-type.properties";

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Taikang Track ChildrenCare APP31");
        conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, 
                TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
        SparkContext sc = new SparkContext(conf);

        
        HiveContext sqlContext = new HiveContext(sc);
        
        RandomUDF.genrandom(sc, sqlContext);
        
        String staticsEventPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH);
		App.loadParquet(sqlContext, staticsEventPath, "F_STATISTICS_EVENT", false);

        
        String factUserInfoPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_USERINFO_INPUTPATH);
        App.loadParquet(sqlContext, factUserInfoPath, "FACT_USERINFO", false);
        
        String pLifeInsurePath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_PLIFEINSURE_PARQUET);
        App.loadParquet(sqlContext, pLifeInsurePath, "P_LIFEINSURE", false);
        
        String pInsurantPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_PINSURANT_PARQUET);
        App.loadParquet(sqlContext, pInsurantPath, "P_INSURANT", false);
        
        String guPolicyMainPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_GUPOLICYCOPYMAIN_PARQUET);
        App.loadParquet(sqlContext, guPolicyMainPath, "GUPOLICYMAIN", false);
        
        String guPolicyRelatedPartyPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_GUPOLICYRELATEDPARTY_PARQUET);
        App.loadParquet(sqlContext, guPolicyRelatedPartyPath, "GUPOLICYRELATEDPARTY", false);
        
        String pCustomerPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_PCUSTOMER_PARQUET);
        App.loadParquet(sqlContext, pCustomerPath, "P_CUSTOMER", false);
        

        
		
        
        String appids = TK_CommonConfig.getConfigValue(CONFIG_PATH, "childrencare.appids");
        String clueyear = TK_CommonConfig.getConfigValue(CONFIG_PATH, "childrencare.clueyear");
		
		System.out.println("=============childrencare.appids====================" + appids);
        
        App.loadFactChildrenCareApp(sqlContext,appids,clueyear);
		sqlContext.clearCache();
		sc.stop();
	}

}
