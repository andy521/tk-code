/**
 * @Title: StatisticsEvent.java
 * @Package: com.tk.track.fact.sparksql.etl
 * @Description: TODO
 * @author zhang.shy
 * @Data 2016年5月7日
 * @version 1.0
 */
package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.desttable.FactStatisticsEvent;
import com.tk.track.fact.sparksql.desttable.FactUserBehaviorClue;
import com.tk.track.fact.sparksql.desttable.TempFactSrcUserEvent;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.IDUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

/**
 * @ClassName: StatisticsEvent
 * @Description: TODO
 * @author zhang.shy
 * @date 2016年5月7日
 */
public class StatisticsEvent implements Serializable {
    
    private static final long serialVersionUID = -6994393927839759188L;
	String app_appid = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_TRACK_SPECIALCLUE_APP_APPID);//配置文件必须以,分隔

    public DataFrame getStatisticsEventDF(HiveContext sqlContext) {
        filterTerminalIDDF(sqlContext);
        filterEventDF(sqlContext);
        getTerminalAndTimeDF(sqlContext);
        fillUserIDDF(sqlContext);
        getDurationDF(sqlContext);
        filldurationDF(sqlContext);
        getTempTotalData(sqlContext);
        getAppDuration(sqlContext);
        getMinAppDuration(sqlContext);
        getTotalData(sqlContext);
        getLatestVisitInfo(sqlContext);
        getLatestFromId(sqlContext);
        calculateVisitInfoDF(sqlContext);
        addFromId2VisitInfo(sqlContext);
        return getStatisticsEventInfoResult(sqlContext);
    }

    private DataFrame filterTerminalIDDF(HiveContext sqlContext) {
        String hql = "SELECT A.IP," 
                + "          A.TIME," 
                + "          A.APP_TYPE," 
                + "          A.APP_ID," 
                + "          A.USER_ID," 
                + "          A.PAGE,"
                + "          A.EVENT,"
                + "          A.SUBTYPE,"
                + "          A.LABEL,"
                + "          A.CUSTOM_VAL," 
                + "          A.REFER," 
                + "          A.CURRENTURL,"
                + "          A.BROWSER," 
                + "          A.SYSINFO,"
                + "          A.TERMINAL_ID,"
                + "          A.DURATION,"
                + "          CASE WHEN A.CLIENTTIME = '' OR A.CLIENTTIME IS NULL THEN A.TIME"
                + "				  ELSE A.CLIENTTIME"
                + "			 END AS CLIENTTIME,"
                + "			 A.FROM_ID"
                + "    FROM  Temp_SrcUserEvent_WithOpenID A"
                + "   WHERE  A.TERMINAL_ID <> '' "
                + "     AND  A.TERMINAL_ID IS NOT NULL"
                + "     AND  A.EVENT <> 'page.load'";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_SRC_USER_EVENT",DataFrameUtil.CACHETABLE_EAGER);
    }

    private DataFrame filterEventDF(HiveContext sqlContext) {
        String hql = "SELECT SUE.IP," 
                + "          SUE.TIME,"
                + "          SUE.APP_TYPE,"
                + "          SUE.APP_ID,"
                + "          SUE.USER_ID,"
                + "          SUE.PAGE,"
                + "          SUE.EVENT,"
                + "          SUE.SUBTYPE," 
                + "          SUE.LABEL," 
                + "          SUE.CUSTOM_VAL," 
                + "          SUE.REFER,"
                + "          SUE.CURRENTURL," 
                + "          SUE.BROWSER," 
                + "          SUE.SYSINFO,"
                + "          SUE.TERMINAL_ID,"
                + "          SUE.DURATION,"
                + "          SUE.CLIENTTIME"
                + "    FROM  TMP_SRC_USER_EVENT SUE" 
                + "   WHERE  SUE.EVENT = 'page.unload' ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_UNLOAD_EVENT",DataFrameUtil.CACHETABLE_EAGER);
    }
    
	private DataFrame getTerminalAndTimeDF(HiveContext sqlContext) {
        String hql = "SELECT TMAP.APP_TYPE,"
        		+ "			 TMAP.APP_ID,"
        		+ "			 SUE.TERMINAL_ID,"
                + "          SUE.CLIENTTIME," 
                + "      CAST(MIN(CAST(TMAP.CLIENTTIME AS BIGINT)) AS STRING) AS NEAR_TIME"
                + "    FROM  TMP_SRC_USER_EVENT SUE, TMP_FACT_TERMINAL_MAP TMAP"
                + "   WHERE  TMAP.TERMINAL_ID = SUE.TERMINAL_ID"
                + "		AND  ((SUE.APP_TYPE = TMAP.APP_TYPE"
                + "     AND  SUE.APP_ID =  TMAP.APP_ID)"
                + "		OR   (TMAP.APP_TYPE = 'IMP'"
                + "     AND  TMAP.APP_ID IN ('IMP_001', 'IMP_002')))" 
                + "     AND  TMAP.CLIENTTIME >= SUE.CLIENTTIME"
                + "   GROUP  BY TMAP.APP_TYPE,TMAP.APP_ID,SUE.TERMINAL_ID, SUE.CLIENTTIME ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_TERMINAL_AND_TIME",DataFrameUtil.CACHETABLE_PARQUET);
    }

    private DataFrame fillUserIDDF(HiveContext sqlContext) {
        fillUserIDPart1DF(sqlContext);
        fillUserIDPart2DF(sqlContext);
        fillUserIDPart3DF(sqlContext);
        return fillUserIDPart4DF(sqlContext);
    }

	private DataFrame fillUserIDPart1DF(HiveContext sqlContext) {
        String hql = "SELECT   FUP0.TIME," 
                + "            FUP0.TERMINAL_ID,"
                + " CASE WHEN  FUP0.USER_ID <> '' AND FUP0.USER_ID IS NOT NULL THEN FUP0.USER_ID"
                + "		 WHEN  FUP0.LABEL LIKE '%openid%' AND FUP0.LABEL NOT LIKE '%openid:\"\"%' THEN regexp_extract(FUP0.LABEL,'(?<=openid:\")[^\",]*',0)"
                + "      WHEN  FUP0.USER_ID = '' OR FUP0.USER_ID IS NULL THEN TMAP.USER_ID" 
                + " END    AS  USER_ID,"
                + "            FUP0.APP_TYPE,"
                + "            FUP0.APP_ID,"
                + "            FUP0.EVENT,"
                + "            FUP0.SUBTYPE,"
                + "            FUP0.LABEL,"
                + "            FUP0.CUSTOM_VAL,"
                + "            FUP0.CURRENTURL,"
                + "            FUP0.CLIENTTIME," 
                + "            FUP0.DURATION,"
                + "			   FUP0.FROM_ID"
                + "      FROM  TMP_FACT_TERMINAL_MAP TMAP, "
                + "            (SELECT SUE.TIME," 
                + "                    SUE.TERMINAL_ID,"
                + "                    SUE.USER_ID,"
                + "                    SUE.APP_TYPE," 
                + "                    SUE.APP_ID," 
                + "                    SUE.EVENT," 
                + "                    SUE.SUBTYPE,"
                + "                    SUE.LABEL,"
                + "                    SUE.CUSTOM_VAL," 
                + "                    SUE.CURRENTURL,"
                + "                    SUE.CLIENTTIME," 
                + "                    SUE.DURATION,"
                + "					   SUE.FROM_ID,"
                + "                    TAT.NEAR_TIME"
                + "              FROM  TMP_SRC_USER_EVENT SUE, TMP_TERMINAL_AND_TIME TAT "
                + "             WHERE  SUE.TERMINAL_ID = TAT.TERMINAL_ID"
                + "				  AND  ((SUE.APP_TYPE = TAT.APP_TYPE"
                + "				  AND  SUE.APP_ID = TAT.APP_ID) OR (TAT.APP_TYPE='IMP' AND TAT.APP_ID='IMP_001')) "
                + "               AND  SUE.CLIENTTIME = TAT.CLIENTTIME "
                + "               AND  SUE.EVENT <> 'page.unload' ) FUP0 "
                + "     WHERE  FUP0.NEAR_TIME = TMAP.CLIENTTIME "
                + "       AND  FUP0.TERMINAL_ID = TMAP.TERMINAL_ID"
                + "		  AND  ((FUP0.APP_TYPE = TMAP.APP_TYPE"
                + "		  AND  FUP0.APP_ID = TMAP.APP_ID) OR (TMAP.APP_TYPE='IMP' AND TMAP.APP_ID='IMP_001'))";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FILL_USERID_PART1");
    }

    // 筛选出各个终端最后一次登陆的信息(无USER_ID信息)
    private DataFrame fillUserIDPart2DF(HiveContext sqlContext) {
        String hql = "SELECT  TMAP.APP_TYPE,"
        		+ "		   TMAP.APP_ID,"
        		+ "		   TMAP.TERMINAL_ID, "
                + "        CAST(MAX(CAST(TMAP.CLIENTTIME AS BIGINT)) AS STRING) AS MAXTIME "
                + "     FROM  TMP_FACT_TERMINAL_MAP TMAP " 
                + " GROUP BY  TMAP.APP_TYPE,TMAP.APP_ID,TMAP.TERMINAL_ID";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FILL_USERID_PART2",DataFrameUtil.CACHETABLE_EAGER);
    }

    // 向下补全： 补全各个终端最后一个非空USER_ID之后的USER_ID值
    // 1) NUID表为从原表中筛选出各个终端的最后一个非空USER_ID之后的空USER_ID信息。
    // 2) FUID表为从TMP_FACT_TERMINAL_MAP表中筛选出各个终端最后一次登陆的信息(有USER_ID信息)
    private DataFrame fillUserIDPart3DF(HiveContext sqlContext) {
        String hql = "SELECT  NUID.TIME, "
                + "           NUID.TERMINAL_ID, "
                + " CASE WHEN FUID.USER_ID <> '' AND FUID.USER_ID IS NOT NULL THEN FUID.USER_ID"
                + "		 WHEN NUID.LABEL LIKE '%openid%' AND NUID.LABEL NOT LIKE '%openid:\"\"%' THEN regexp_extract(NUID.LABEL,'(?<=openid:\")[^\",]*',0)"
                + " END AS    USER_ID, "
                + "           NUID.APP_TYPE,"
                + "           NUID.APP_ID,"
                + "           NUID.EVENT,"
                + "           NUID.SUBTYPE,"
                + "           NUID.LABEL,"
                + "           NUID.CUSTOM_VAL,"
                + "           NUID.CURRENTURL,"
                + "           NUID.CLIENTTIME,"
                + "           NUID.DURATION,"
                + "			  NUID.FROM_ID" 
                + "     FROM  (SELECT   SUE.* "
                + "               FROM  TMP_SRC_USER_EVENT SUE, TMP_FILL_USERID_PART2 FUP2"
                + "              WHERE  SUE.TERMINAL_ID = FUP2.TERMINAL_ID"
                + "				   AND  SUE.APP_TYPE = FUP2.APP_TYPE"
                + "				   AND  SUE.APP_ID = FUP2.APP_ID "
                + "                AND  SUE.CLIENTTIME > FUP2.MAXTIME"
                + "                AND  SUE.EVENT <> 'page.unload') NUID, "
                + "           (SELECT   TMAP.* "
                + "               FROM  TMP_FACT_TERMINAL_MAP TMAP "
                + "         INNER JOIN  TMP_FILL_USERID_PART2 FUP2" 
                + "                 ON  TMAP.TERMINAL_ID = FUP2.TERMINAL_ID "
                + "				   AND  ((TMAP.APP_TYPE = FUP2.APP_TYPE"
                + "				   AND  TMAP.APP_ID = FUP2.APP_ID) OR "
                + "				   (TMAP.APP_TYPE='IMP' AND TMAP.APP_ID='IMP_001'))"
                + "                AND  TMAP.CLIENTTIME = FUP2.MAXTIME) FUID "
                + "    WHERE  NUID.TERMINAL_ID = FUID.TERMINAL_ID"
                + "	    AND   NUID.APP_TYPE = FUID.APP_TYPE"
                + "	    AND   NUID.APP_ID = FUID.APP_ID";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FILL_USERID_PART3");
    }

    private DataFrame fillUserIDPart4DF(HiveContext sqlContext) {
        String hql = "SELECT   FUP1.TIME, " 
                + "            FUP1.TERMINAL_ID,"
                + "            FUP1.USER_ID," 
                + "            FUP1.APP_TYPE,"
                + "            FUP1.APP_ID," 
                + "            FUP1.EVENT," 
                + "            FUP1.SUBTYPE,"
                + "            FUP1.LABEL,"
                + "            FUP1.CUSTOM_VAL,"
                + "            FUP1.CURRENTURL,"
                + "            FUP1.CLIENTTIME," 
                + "            FUP1.DURATION,"
                + "			   FUP1.FROM_ID"
                + "      FROM  TMP_FILL_USERID_PART1 FUP1 "
                + " UNION ALL " 
                + "    SELECT  FUP3.TIME,"
                + "            FUP3.TERMINAL_ID,"
                + "            FUP3.USER_ID," 
                + "            FUP3.APP_TYPE,"
                + "            FUP3.APP_ID,"
                + "            FUP3.EVENT,"
                + "            FUP3.SUBTYPE," 
                + "            FUP3.LABEL,"
                + "            FUP3.CUSTOM_VAL,"
                + "            FUP3.CURRENTURL,"
                + "            FUP3.CLIENTTIME,"
                + "            FUP3.DURATION,"
                + "			   FUP3.FROM_ID"
                + "      FROM  TMP_FILL_USERID_PART3 FUP3 ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FILL_USERID",DataFrameUtil.CACHETABLE_PARQUET);
    }
    
    private DataFrame fillUserTypeToLabel(HiveContext sqlContext) {
    	String hql = "SELECT   TFU.TIME, " 
    			+ "            TFU.TERMINAL_ID,"
    			+ "            SUBSTR(TRIM(TFU.USER_ID), 4) AS USER_ID," 
    			+ "            TFU.APP_TYPE,"
    			+ "            TFU.APP_ID," 
    			+ "            TFU.EVENT," 
    			+ "            TFU.SUBTYPE,"
    			+ "  CASE WHEN SUBSTR(TRIM(TFU.USER_ID), 0, 3)='MEM' THEN CONCAT(TFU.LABEL, ',userType:MEM')"
    			+ "       WHEN SUBSTR(TRIM(TFU.USER_ID), 0, 3)='CUS' THEN CONCAT(TFU.LABEL, ',userType:CUS')"
    			+ "       WHEN SUBSTR(TRIM(TFU.USER_ID), 0, 3)='PHO' THEN CONCAT(TFU.LABEL, ',userType:PHO')"
    			+ "       ELSE TFU.LABEL"
    			+ "    END  AS LABEL,"
    			+ "            TFU.CUSTOM_VAL,"
    			+ "            TFU.CURRENTURL,"
    			+ "            TFU.CLIENTTIME," 
    			+ "            TFU.DURATION,"
    			+ "			   TFU.FROM_ID"
    			+ "      FROM  TMP_FILL_USERID TFU ";
    	return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FILL_USERID",DataFrameUtil.CACHETABLE_PARQUET);
    }

    private DataFrame getDurationDF(HiveContext sqlContext) {
        getDurationPart1DF(sqlContext);
        getDurationPart2DF(sqlContext);
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
        String hql = "SELECT  TD.USER_ID," 
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
                + " GROUP BY  TD.USER_ID, TD.APP_TYPE, TD.APP_ID, TD.EVENT,"
                + "           TD.SUBTYPE, TD.LABEL, TD.CUSTOM_VAL";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_CALCULATE_VISIT_INFO",DataFrameUtil.CACHETABLE_PARQUET);
    }
    
    private DataFrame getLatestVisitInfo(HiveContext sqlContext){
    	String hql = "SELECT  "
        		+ "			  CAST(MAX(CAST(TDATA.CLIENTTIME AS BIGINT)) AS STRING) AS CLIENTTIME, "
                + "           TDATA.USER_ID"
                + "      FROM TMP_TOTAL_DATA TDATA"
                + "	  WHERE TDATA.FROM_ID <> '' AND TDATA.FROM_ID IS NOT NULL"
                + "    GROUP BY "
                + "			    TDATA.USER_ID";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_LATEST");
    }
    
    private DataFrame getLatestFromId(HiveContext sqlContext){
    	String sql = "SELECT LT.USER_ID,"
    			+ "			 MIN(TDATA.FROM_ID) AS FROM_ID"
    			+ "	    FROM TMP_LATEST LT,TMP_TOTAL_DATA TDATA"
    			+ "    WHERE LT.CLIENTTIME = TDATA.CLIENTTIME"
    			+ " GROUP BY LT.USER_ID";
    	return DataFrameUtil.getDataFrame(sqlContext, sql, "TMP_LATESTFROM");
    }
    
    private DataFrame addFromId2VisitInfo(HiveContext sqlContext) {
        String hql = "SELECT  TD.USER_ID," 
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
                + "       ON  TD.USER_ID = LTF.USER_ID";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_VISIT_INFO");
    }

    private DataFrame getStatisticsEventInfoResult(HiveContext sqlContext) {//最后去重，同一最早VISIT_TIME只能有一条数据
        String hql = "SELECT  *"
                + "     FROM  TMP_VISIT_INFO"
                + "    GROUP  BY USER_ID," 
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

    public JavaRDD<FactStatisticsEvent> getJavaRDD(DataFrame df) {
        RDD<Row> rdd = df.select("USER_ID", "APP_TYPE", "APP_ID", "EVENT", "SUBTYPE", "LABEL", "CUSTOM_VAL",
                "VISIT_COUNT", "VISIT_TIME", "VISIT_DURATION","FROM_ID").rdd();
        JavaRDD<Row> jRDD = rdd.toJavaRDD();

        JavaRDD<FactStatisticsEvent> pRDD = jRDD.map(new Function<Row, FactStatisticsEvent>() {
            private static final long serialVersionUID = -2370716651591622510L;

            public FactStatisticsEvent call(Row v1) throws Exception {
                String rowKey = "";
                rowKey = IDUtil.getUUID().toString();
                return new FactStatisticsEvent(rowKey, v1.getString(0), v1.getString(1), v1.getString(2),
                        v1.getString(3), v1.getString(4), v1.getString(5), v1.getString(6),
                        Long.toString(v1.getLong(7)), v1.getString(8), v1.getString(9),v1.getString(10));
            }
        });

        return pRDD;
    }

	public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<FactStatisticsEvent> rdd, String path) {
		if (TK_DataFormatConvertUtil.isExistsPath(path)) {
			String tmpPath = path + "-temp";
			Integer repartition_count = 200;
			try {
				repartition_count = Integer.valueOf(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_REPARTITION_COUNT));
			} catch (Exception e) {
				repartition_count = 200;
			}
			
			// Append new data to parquet[path]
			sqlContext.createDataFrame(rdd, FactStatisticsEvent.class).save(path, "parquet", SaveMode.Append);
			// Load parquet[path], save as parquet[tmpPath]
			TK_DataFormatConvertUtil.deletePath(tmpPath);
			sqlContext.load(path).repartition(repartition_count).saveAsParquetFile(tmpPath);
			// Delete parquet[path]
			TK_DataFormatConvertUtil.deletePath(path);
			// Rename parquet[tmpPath] as parquet[path]
			TK_DataFormatConvertUtil.renamePath(tmpPath, path);
		} else {
			sqlContext.createDataFrame(rdd, FactStatisticsEvent.class).saveAsParquetFile(path);
		}
	}
}
