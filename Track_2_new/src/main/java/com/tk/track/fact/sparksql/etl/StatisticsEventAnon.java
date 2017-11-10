/**
 * @Title: StatisticsEvent.java
 * @Package: com.tk.track.fact.sparksql.etl
 * @Description: TODO
 * @author zhang.shy
 * @Data 2016年5月7日
 * @version 1.0
 */
package com.tk.track.fact.sparksql.etl;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.desttable.FactStatisticsEventWithAnony;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.IDUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import java.io.Serializable;

/**
 * @ClassName: StatisticsEvent
 * @Description: TODO
 * @author zhang.shy
 * @date 2016年5月7日
 */
public class StatisticsEventAnon implements Serializable {

    private static final long serialVersionUID = -6994393927839759188L;
	String app_appid = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_TRACK_SPECIALCLUE_APP_APPID);//配置文件必须以,分隔
	private static final String days = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_RECOMEND_USERINFO_TIMERECENT);

    public DataFrame getStatisticsEventDFAnony(HiveContext sqlContext) {
    	DataFrame dfAnony=findAnonyUser(sqlContext);
//        saveAsParquet(dfAnony, "/user/tkonline/taikangtrack/data/fact_statics_anony");
    	
    	findAnonyUserExceptUnload(sqlContext);
    	filterEventDF(sqlContext);
    	
        DataFrame dura3=getDurationDF(sqlContext);
//        saveAsParquet(dura3, "/user/tkonline/taikangtrack/data/fact_statics_dura3");
        
        DataFrame filldura=filldurationDF(sqlContext);
        
//        saveAsParquet(filldura, "/user/tkonline/taikangtrack/data/fact_statics_filldura");
        
        DataFrame tempTotal=getTempTotalData(sqlContext);
//        saveAsParquet(tempTotal, "/user/tkonline/taikangtrack/data/fact_statics_tempTotal");
        
        DataFrame appDura=getAppDuration(sqlContext);
//        saveAsParquet(appDura, "/user/tkonline/taikangtrack/data/fact_statics_appDura");
        
        
        DataFrame minApp=getMinAppDuration(sqlContext);
//        saveAsParquet(minApp, "/user/tkonline/taikangtrack/data/fact_statics_minAppDura");
        
        DataFrame df14=getTotalData(sqlContext);
//        saveAsParquet(df14, "/user/tkonline/taikangtrack/data/fact_statics_anonyt14");
        
        getLatestVisitInfo(sqlContext);
        getLatestFromId(sqlContext);
        
        DataFrame dfcalu=calculateVisitInfoDF(sqlContext);
//        saveAsParquet(dfcalu, "/user/tkonline/taikangtrack/data/fact_statics_dfcalu");
        
        DataFrame dffromId=addFromId2VisitInfo(sqlContext);
//        saveAsParquet(dffromId, "/user/tkonline/taikangtrack/data/fact_statics_dffromId");
        return getStatisticsEventInfoResultAnony(sqlContext);
    }
    /**
	 * 找到从未登录过的匿名用户
	 * @param sqlContext
	 * @param tableName
	 * @return
	 */
	private DataFrame findAnonyUser(HiveContext sqlContext) {
		
		String sql="" +
				"SELECT UBL.TIME,UBL.TERMINAL_ID,UBL.USER_ID,UBL.APP_TYPE,UBL.APP_ID,UBL.SYSINFO," + 
				"UBL.EVENT,UBL.SUBTYPE,UBL.LABEL,UBL.CUSTOM_VAL,UBL.CURRENTURL," + 
				"UBL.CLIENTTIME,UBL.DURATION,UBL.IP,UBL.FROM_ID " + 
				"FROM UBA_LOG_EVENT UBL " +
				"WHERE  UBL.TERMINAL_ID<>'' AND UBL.TERMINAL_ID IS NOT NULL AND (UBL.USER_ID IS NULL OR UBL.USER_ID='') " + 
				" AND DATEDIFF(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss'),FROM_UNIXTIME(CAST(CAST(UBL.time AS BIGINT) / 1000 AS BIGINT),'yyyy-MM-dd')) <= "+days+" "+
				"AND  UBL.EVENT <> 'PAGE.LOAD' " ;
			
        return DataFrameUtil.getDataFrame(sqlContext, sql, "AnonyUserBrowseInfo", DataFrameUtil.CACHETABLE_PARQUET);
	}
//	/**
//	 * 找到从未登录过的匿名用户 的 unload事件
//	 * @param sqlContext
//	 * @param tableName
//	 * @return
//	 */
//	private DataFrame findAnonyUserUnload(HiveContext sqlContext,String tableName) {
//		String sql = "select * from AnonyUserBrowseInfo aubf where aubf.EVENT = 'page.unload' ";
//			
//		return DataFrameUtil.getDataFrame(sqlContext, sql, "UserBrowseInfo_unload");
//	}
	
	/**
	 *  找到从未登录过的匿名用户 的 非unload事件
	 *  
	 * @param sqlContext
	 * @return TMP_FILL_USERID 表名，实际user_id都为空，此处延用旧有代码名
	 */
	private DataFrame findAnonyUserExceptUnload(HiveContext sqlContext) {
		String sql = "select * from AnonyUserBrowseInfo aubf where aubf.EVENT <> 'page.unload' ";
			
		return DataFrameUtil.getDataFrame(sqlContext, sql, "TMP_FILL_USERID");
	}
	/**
	 * 找到从未登录过的匿名用户 的 unload事件
	 * @param sqlContext
	 * @param tableName
	 * @return
	 */
    private DataFrame filterEventDF(HiveContext sqlContext) {
        String hql = "SELECT SUE.IP," 
                + "          SUE.TIME,"
                + "          SUE.APP_TYPE,"
                + "          SUE.APP_ID,"
                + "          SUE.USER_ID,"
                + "          SUE.EVENT,"
                + "          SUE.SUBTYPE," 
                + "          SUE.LABEL," 
                + "          SUE.CUSTOM_VAL," 
                + "          SUE.CURRENTURL,"  
                + "          SUE.SYSINFO,"
                + "          SUE.TERMINAL_ID,"
                + "          SUE.DURATION,"
                + "          SUE.CLIENTTIME"
                + "    FROM  AnonyUserBrowseInfo SUE" 
                + "   WHERE  SUE.EVENT = 'page.unload' ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_UNLOAD_EVENT",DataFrameUtil.CACHETABLE_EAGER);
    }


    private DataFrame getDurationDF(HiveContext sqlContext) {
        DataFrame dura1=getDurationPart1DF(sqlContext);
//        saveAsParquet(dura1, "/user/tkonline/taikangtrack/data/fact_statics_dura1");
        
        DataFrame dura2=getDurationPart2DF(sqlContext);
//        saveAsParquet(dura2, "/user/tkonline/taikangtrack/data/fact_statics_dura2");
        return getDurationPart3DF(sqlContext);
    }
    
    private DataFrame getDurationPart1DF(HiveContext sqlContext) {
        String hql = "SELECT  FUID.TERMINAL_ID, "
                + "           FUID.APP_TYPE, "
                + "           FUID.APP_ID, "
                + "           FUID.CURRENTURL, "
                + "           CAST(MAX(CAST(FUID.CLIENTTIME AS BIGINT)) AS STRING) AS CLIENTTIME,"
                + "           ULE.CLIENTTIME AS UNLOADTIME"
                + "      FROM TMP_FILL_USERID FUID "
                + " LEFT JOIN TMP_UNLOAD_EVENT ULE "
                + "      ON   CASE WHEN LOWER(FUID.APP_ID) in (" + app_appid.toLowerCase() + ") THEN FUID.CUSTOM_VAL = '-2' "//当为特殊APP时只取AppWap
                + "           ELSE 1 = 1 END "
                + "      AND  FUID.TERMINAL_ID = ULE.TERMINAL_ID "
                + "      AND  FUID.APP_TYPE = ULE.APP_TYPE "
                + "      AND  FUID.APP_ID = ULE.APP_ID "
                + "      AND  FUID.CURRENTURL = ULE.CURRENTURL "
                + "      AND  FUID.CLIENTTIME < ULE.CLIENTTIME "
                + " GROUP BY  FUID.TERMINAL_ID, FUID.APP_TYPE, FUID.APP_ID, FUID.CURRENTURL, ULE.CLIENTTIME";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_DURATION_PART1");
    }

    private DataFrame getDurationPart2DF(HiveContext sqlContext) {
        String hql = "SELECT  ULE.TERMINAL_ID, " 
                + "           ULE.APP_TYPE, " 
                + "           ULE.APP_ID, "
                + "           ULE.CURRENTURL, "
                + "           CAST(MIN(CAST(ULE.CLIENTTIME AS BIGINT)) AS STRING) AS MIN_TTIME, "
                + "           DP1.CLIENTTIME "
                + "      FROM TMP_DURATION_PART1 DP1 "
                + " LEFT JOIN TMP_UNLOAD_EVENT ULE" 
                + "      ON   DP1.TERMINAL_ID = ULE.TERMINAL_ID "
                + "      AND  DP1.APP_TYPE = ULE.APP_TYPE " 
                + "      AND  DP1.APP_ID = ULE.APP_ID "
                + "      AND  DP1.CURRENTURL = ULE.CURRENTURL" 
                + "      AND  DP1.CLIENTTIME < ULE.CLIENTTIME "
                + " GROUP BY  ULE.TERMINAL_ID, ULE.APP_TYPE, ULE.APP_ID, ULE.CURRENTURL, DP1.CLIENTTIME";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_DURATION_PART2");
    }

    private DataFrame getDurationPart3DF(HiveContext sqlContext) {
        String hql = "SELECT  DP2.TERMINAL_ID, " 
                + "           DP2.APP_TYPE, "
                + "           DP2.APP_ID, " 
                + "           DP2.CURRENTURL, "
                + "           ULE.DURATION, "
                + "           DP2.CLIENTTIME "
                + "      FROM TMP_DURATION_PART2 DP2 "
                + " LEFT JOIN TMP_UNLOAD_EVENT ULE"
                + "      ON   DP2.TERMINAL_ID = ULE.TERMINAL_ID "
                + "      AND  DP2.APP_TYPE = ULE.APP_TYPE "
                + "      AND  DP2.APP_ID = ULE.APP_ID "
                + "      AND  DP2.CURRENTURL = ULE.CURRENTURL "
                + "      AND  DP2.MIN_TTIME = ULE.CLIENTTIME";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_DURATION_PART3");
    }
    
    private DataFrame filldurationDF(HiveContext sqlContext) {
        String hql = "SELECT  FUID.TIME, "
                + "           FUID.TERMINAL_ID,"
                + "           FUID.USER_ID,"
                + "           FUID.APP_TYPE,"
                + "           FUID.APP_ID,"
                + "           FUID.EVENT," 
                + "           FUID.SUBTYPE,"
                + "           FUID.LABEL," 
                + "           FUID.CUSTOM_VAL,"
                + "           FUID.CURRENTURL," 
                + "           FUID.CLIENTTIME," 
                + "           DP3.DURATION"
                + "     FROM  TMP_FILL_USERID FUID, TMP_DURATION_PART3 DP3 "
                + "    WHERE  FUID.TERMINAL_ID = DP3.TERMINAL_ID "
                + "      AND  FUID.APP_TYPE = DP3.APP_TYPE " 
                + "      AND  FUID.APP_ID = DP3.APP_ID "
                + "      AND  FUID.CURRENTURL = DP3.CURRENTURL "
                + "      AND  FUID.CLIENTTIME = DP3.CLIENTTIME";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FILL_DURATION", DataFrameUtil.CACHETABLE_PARQUET);
    }
    
    private DataFrame getTempTotalData(HiveContext sqlContext) {
        String hql = "SELECT   FUID.FROM_ID,"
        		+ "			   FUID.TIME, "
                + "            FUID.TERMINAL_ID,"
                + "            FUID.USER_ID,"
                + "            FUID.APP_TYPE,"
                + "            FUID.APP_ID,"
                + "            FUID.EVENT," 
                + "            FUID.SUBTYPE,"
                + "            FUID.LABEL," 
                + "            FUID.CUSTOM_VAL,"
                + "            FUID.CURRENTURL," 
                + "            FUID.CLIENTTIME," 
                + "            FUID.DURATION,"
                + "            FD.DURATION AS TMP_DURATION"
                + "      FROM  TMP_FILL_USERID FUID "
                + " LEFT JOIN  TMP_FILL_DURATION FD"
                + "        ON  FUID.TERMINAL_ID = FD.TERMINAL_ID "
                + "       AND  FUID.APP_TYPE = FD.APP_TYPE "
                + "       AND  FUID.APP_ID = FD.APP_ID " 
                + "       AND  FUID.CURRENTURL = FD.CURRENTURL "
                + "       AND  FUID.CLIENTTIME = FD.CLIENTTIME";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_TOTAL_DATA_1");
    }
    
    //AppDURATION
    private DataFrame getAppDuration(HiveContext sqlContext) {
        String hql = "SELECT   A.FROM_ID, "
        		+ "			   A.TIME, "
                + "            A.TERMINAL_ID, "
                + "            A.USER_ID, "
                + "            A.APP_TYPE, "
                + "            A.APP_ID, "
                + "            A.EVENT, " 
                + "            A.SUBTYPE, "
                + "            A.LABEL, " 
                + "            A.CUSTOM_VAL, "
                + "            A.CURRENTURL, " 
                + "            A.CLIENTTIME, " 
                + "            CAST((CAST(B.CLIENTTIME AS INT) - CAST(A.CLIENTTIME AS INT)) AS STRING) AS APPDURATION "
                + "      FROM  TMP_FILL_USERID A, TMP_FILL_USERID B "
                + "     WHERE  CAST(A.CUSTOM_VAL AS INT) = (CAST(B.CUSTOM_VAL AS INT) - 1) "
                + "       AND  A.TIME <= B.TIME "
                + "       AND  A.CLIENTTIME <= B.CLIENTTIME "
                + "       AND  A.FROM_ID = B.FROM_ID "
                + "       AND  A.TERMINAL_ID = B.TERMINAL_ID "
                + "       AND  A.USER_ID = B.USER_ID "
                + "       AND  A.APP_TYPE = B.APP_TYPE "
                + "       AND  A.EVENT = B.EVENT "
                + "       AND  A.SUBTYPE = B.SUBTYPE "
                + "       AND  A.LABEL = B.LABEL "
                + "       AND  A.CURRENTURL = B.CURRENTURL";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_APP_DURATION");
    }
    
    private DataFrame getMinAppDuration(HiveContext sqlContext) {
        String hql = "SELECT   FROM_ID, "
        		+ "			   TIME, "
                + "            TERMINAL_ID, "
                + "            USER_ID, "
                + "            APP_TYPE, "
                + "            APP_ID, "
                + "            EVENT, " 
                + "            SUBTYPE, "
                + "            LABEL, " 
                + "            CUSTOM_VAL, "
                + "            CURRENTURL, " 
                + "            CLIENTTIME, " 
                + "            CAST(MIN(CAST(APPDURATION AS BIGINT)) AS STRING) AS APPDURATION "
                + "      FROM  TMP_APP_DURATION "
                + "  GROUP BY  FROM_ID, "
        		+ "			   TIME, "
                + "            TERMINAL_ID, "
                + "            USER_ID, "
                + "            APP_TYPE, "
                + "            APP_ID, "
                + "            EVENT, " 
                + "            SUBTYPE, "
                + "            LABEL, " 
                + "            CUSTOM_VAL, "
                + "            CURRENTURL, " 
                + "            CLIENTTIME";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "APP_DURATION");
    }
    
    //合并
    private DataFrame getTotalData(HiveContext sqlContext) {
        String hql = "SELECT  TDATA.FROM_ID,"
        		+ "			  TDATA.TIME, "
                + "           TDATA.TERMINAL_ID,"
                + "           TDATA.USER_ID,"
                + "           TDATA.APP_TYPE,"
                + "           TDATA.APP_ID,"
                + "           TDATA.EVENT," 
                + "           TDATA.SUBTYPE,"
                + "           TDATA.LABEL," 
                + "           TDATA.CUSTOM_VAL,"
                + "           TDATA.CURRENTURL," 
                + "           TDATA.CLIENTTIME," 
                + " CASE WHEN  TDATA.TMP_DURATION <> '' AND TDATA.TMP_DURATION IS NOT NULL THEN TDATA.TMP_DURATION "
                + "      WHEN  LOWER(TDATA.APP_ID) in (" + app_appid.toLowerCase() + ") AND AD.CUSTOM_VAL IS NOT NULL AND TRIM(AD.CUSTOM_VAL) <> '' AND TDATA.CUSTOM_VAL <> '-2' THEN AD.APPDURATION "
                + "      ELSE  '0' "//若取不到，则赋零
                + " END    AS  DURATION"
                + "      From TMP_TOTAL_DATA_1 TDATA"
                + " LEFT JOIN APP_DURATION AD"
                + "        ON TDATA.FROM_ID = AD.FROM_ID "
        		+ "		  AND TDATA.TIME = AD.TIME "
                + "       AND TDATA.TERMINAL_ID = AD.TERMINAL_ID "
                + "       AND TDATA.USER_ID = AD.USER_ID "
                + "       AND TDATA.APP_TYPE = AD.APP_TYPE "
                + "       AND TDATA.APP_ID = AD.APP_ID "
                + "       AND TDATA.EVENT = AD.EVENT "
                + "       AND TDATA.SUBTYPE = AD.SUBTYPE "
                + "       AND TDATA.LABEL = AD.LABEL "
                + "       AND TDATA.CUSTOM_VAL = AD.CUSTOM_VAL "
                + "       AND TDATA.CURRENTURL = AD.CURRENTURL "
                + "       AND TDATA.CLIENTTIME = AD.CLIENTTIME";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_TOTAL_DATA",DataFrameUtil.CACHETABLE_PARQUET);
    }

    private DataFrame calculateVisitInfoDF(HiveContext sqlContext) {
        String hql = "SELECT TD.TERMINAL_ID, "
        		+ "           TD.USER_ID," 
                + "           TD.APP_TYPE," 
                + "           TD.APP_ID,"
                + "           TD.EVENT," 
                + "           TD.SUBTYPE,"
                + "           TD.LABEL,"
                + "           TD.CUSTOM_VAL,"
                + "           COUNT(*) AS VISIT_COUNT,"
                + "           CAST(MIN(CAST(TD.TIME AS BIGINT)) AS STRING) AS VISIT_TIME,"
                + "           CAST(SUM(CAST(TD.DURATION AS BIGINT)) AS STRING) AS VISIT_DURATION"
                + "     FROM  TMP_TOTAL_DATA TD " 
                + " GROUP BY  TD.TERMINAL_ID,TD.USER_ID, TD.APP_TYPE, TD.APP_ID, TD.EVENT,"
                + "           TD.SUBTYPE, TD.LABEL, TD.CUSTOM_VAL";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_CALCULATE_VISIT_INFO",DataFrameUtil.CACHETABLE_PARQUET);
    }
    
    private DataFrame getLatestVisitInfo(HiveContext sqlContext){
    	String hql = "SELECT  "
        		+ "			  CAST(MAX(CAST(TDATA.CLIENTTIME AS BIGINT)) AS STRING) AS CLIENTTIME, "
                + "           TDATA.TERMINAL_ID"
                + "      FROM TMP_TOTAL_DATA TDATA"
                + "	  WHERE TDATA.FROM_ID <> '' AND TDATA.FROM_ID IS NOT NULL"
                + " and TDATA.CLIENTTIME <>'' and TDATA.CLIENTTIME is not null" + 
                " and TDATA.TERMINAL_ID <>'' and TDATA.TERMINAL_ID is not null"
                + "    GROUP BY "
                + "			    TDATA.TERMINAL_ID";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_LATEST");
    }
    
    private DataFrame getLatestFromId(HiveContext sqlContext){
    	String sql = "SELECT LT.TERMINAL_ID,"
    			+ "			 MIN(TDATA.FROM_ID) AS FROM_ID"
    			+ "	    FROM TMP_LATEST LT,TMP_TOTAL_DATA TDATA"
    			+ "    WHERE LT.CLIENTTIME = TDATA.CLIENTTIME"
    			+ " GROUP BY LT.TERMINAL_ID";
    	return DataFrameUtil.getDataFrame(sqlContext, sql, "TMP_LATESTFROM");
    }
    
    private DataFrame addFromId2VisitInfo(HiveContext sqlContext) {
        String hql = "SELECT  TD.TERMINAL_ID,"
        		+ "           TD.USER_ID," 
                + "           TD.APP_TYPE," 
                + "           TD.APP_ID,"
                + "           TD.EVENT," 
                + "           TD.SUBTYPE,"
                + "           TD.LABEL,"
                + "           TD.CUSTOM_VAL,"
                + "      	  TD.VISIT_COUNT,"
                + "           TD.VISIT_TIME,"
                + "           TD.VISIT_DURATION,"
                + "			  LTF.FROM_ID"
                + "     FROM  TMP_CALCULATE_VISIT_INFO TD "
                + "	  LEFT JOIN TMP_LATESTFROM LTF"
                + "       ON  TD.TERMINAL_ID = LTF.TERMINAL_ID";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_VISIT_INFO");
    }

    private DataFrame getStatisticsEventInfoResultAnony(HiveContext sqlContext) {//最后去重，同一最早VISIT_TIME只能有一条数据
        String hql = "SELECT  *"
                + "     FROM  TMP_VISIT_INFO"
                + "    GROUP  BY TERMINAL_ID,"
                + "           USER_ID," 
                + "           APP_TYPE," 
                + "           APP_ID,"
                + "           EVENT," 
                + "           SUBTYPE,"
                + "           LABEL,"
                + "           CUSTOM_VAL,"
                + "      	  VISIT_COUNT,"
                + "           VISIT_TIME,"
                + "           VISIT_DURATION,"
                + "			  FROM_ID";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FACT_STATISTICS_EVENT");
    }

    public JavaRDD<FactStatisticsEventWithAnony> getJavaRDD(DataFrame df) {
        RDD<Row> rdd = df.select("TERMINAL_ID","USER_ID", "APP_TYPE", "APP_ID", "EVENT", "SUBTYPE", "LABEL", "CUSTOM_VAL",
                "VISIT_COUNT", "VISIT_TIME", "VISIT_DURATION","FROM_ID").rdd();
        JavaRDD<Row> jRDD = rdd.toJavaRDD();

        JavaRDD<FactStatisticsEventWithAnony> pRDD = jRDD.map(new Function<Row, FactStatisticsEventWithAnony>() {

			private static final long serialVersionUID = -54920468066531895L;

			public FactStatisticsEventWithAnony call(Row v1) throws Exception {
                String rowKey = "";
                rowKey = IDUtil.getUUID().toString();
                return new FactStatisticsEventWithAnony(rowKey,v1.getString(0), v1.getString(1), v1.getString(2), v1.getString(3),
                        v1.getString(4), v1.getString(5), v1.getString(6), v1.getString(7),
                        Long.toString(v1.getLong(8)), v1.getString(9), v1.getString(10),v1.getString(11));
            }
        });

        return pRDD;
    }

	public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<FactStatisticsEventWithAnony> rdd, String path) {
		if (TK_DataFormatConvertUtil.isExistsPath(path)) {
			String tmpPath = path + "-temp";
			Integer repartition_count = 200;
			try {
				repartition_count = Integer.valueOf(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_REPARTITION_COUNT));
			} catch (Exception e) {
				repartition_count = 200;
			}
			
			// Append new data to parquet[path]
			sqlContext.createDataFrame(rdd, FactStatisticsEventWithAnony.class).save(path, "parquet", SaveMode.Append);
			// Load parquet[path], save as parquet[tmpPath]
			TK_DataFormatConvertUtil.deletePath(tmpPath);
			sqlContext.load(path).repartition(repartition_count).saveAsParquetFile(tmpPath);
			// Delete parquet[path]
			TK_DataFormatConvertUtil.deletePath(path);
			// Rename parquet[tmpPath] as parquet[path]
			TK_DataFormatConvertUtil.renamePath(tmpPath, path);
		} else {
			sqlContext.createDataFrame(rdd, FactStatisticsEventWithAnony.class).saveAsParquetFile(path);
		}
	}
	public void saveAsParquet(DataFrame df, String path) {
        TK_DataFormatConvertUtil.deletePath(path);
        df.saveAsParquetFile(path);
	}
}
