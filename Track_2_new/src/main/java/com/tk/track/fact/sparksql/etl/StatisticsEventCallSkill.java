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
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.IDUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

/**
 * @ClassName: StatisticsEventCallSkill
 * @Description: TODO
 * @author liran
 * @date 2016年8月18日
 */
public class StatisticsEventCallSkill implements Serializable {
    
    private static final long serialVersionUID = -6994393927839759188L;
    
    public DataFrame getStatisticsEventDF(HiveContext sqlContext,String appids) {
        filterTerminalIDDF(sqlContext,appids);
        filterEventDF(sqlContext);
        getDurationDF(sqlContext);
        filldurationDF(sqlContext);
        getTempTotalData(sqlContext);
        getTotalData(sqlContext);
        calculateVisitInfoDF(sqlContext);
        return getStatisticsEventInfoResult(sqlContext);
    }

    private DataFrame filterTerminalIDDF(HiveContext sqlContext,String appids) {
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
                + "			 END AS CLIENTTIME"
                + "    FROM  Temp_SrcUserEvent A"
                + "   WHERE  A.EVENT <> 'page.load'"
                + "     AND  LOWER(A.APP_TYPE) = 'system' "
                + "     AND  LOWER(A.APP_ID) in ("+ appids.toLowerCase() +")";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_SRC_USER_EVENT_CS",DataFrameUtil.CACHETABLE_PARQUET);
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
                + "    FROM  TMP_SRC_USER_EVENT_CS SUE" 
                + "   WHERE  SUE.EVENT = 'page.unload' ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_UNLOAD_EVENT_CS",DataFrameUtil.CACHETABLE_PARQUET);
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
                + "      FROM TMP_SRC_USER_EVENT_CS FUID "
                + " LEFT JOIN TMP_UNLOAD_EVENT_CS ULE "
                + "      ON   FUID.TERMINAL_ID = ULE.TERMINAL_ID "
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
                + " LEFT JOIN TMP_UNLOAD_EVENT_CS ULE" 
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
                + " LEFT JOIN TMP_UNLOAD_EVENT_CS ULE"
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
                + "     FROM  TMP_SRC_USER_EVENT_CS FUID, TMP_DURATION_PART3 DP3 "
                + "    WHERE  FUID.TERMINAL_ID = DP3.TERMINAL_ID "
                + "      AND  FUID.APP_TYPE = DP3.APP_TYPE " 
                + "      AND  FUID.APP_ID = DP3.APP_ID "
                + "      AND  FUID.CURRENTURL = DP3.CURRENTURL "
                + "      AND  FUID.CLIENTTIME = DP3.CLIENTTIME";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FILL_DURATION", DataFrameUtil.CACHETABLE_PARQUET);
    }
    
    private DataFrame getTempTotalData(HiveContext sqlContext) {
        String hql = "SELECT   FUID.TIME, "
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
                + "      FROM  TMP_SRC_USER_EVENT_CS FUID "
                + " LEFT JOIN  TMP_FILL_DURATION FD"
                + "        ON  FUID.TERMINAL_ID = FD.TERMINAL_ID "
                + "       AND  FUID.APP_TYPE = FD.APP_TYPE "
                + "       AND  FUID.APP_ID = FD.APP_ID " 
                + "       AND  FUID.CURRENTURL = FD.CURRENTURL "
                + "       AND  FUID.CLIENTTIME = FD.CLIENTTIME";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_TOTAL_DATA_1");
    }
    
    //合并
    private DataFrame getTotalData(HiveContext sqlContext) {
        String hql = "SELECT  TDATA.TIME, "
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
                + " CASE WHEN TDATA.TMP_DURATION <> '' AND TDATA.TMP_DURATION IS NOT NULL THEN TDATA.TMP_DURATION "
                + "      ELSE '0' "//若取不到，则赋零
                + " END    AS DURATION"
                + "      From TMP_TOTAL_DATA_1 TDATA";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_TOTAL_DATA",DataFrameUtil.CACHETABLE_PARQUET);
    }

    private DataFrame calculateVisitInfoDF(HiveContext sqlContext) {
        String hql = "SELECT  TD.USER_ID AS USER_ID," 
                + "           TD.APP_TYPE AS APP_TYPE," 
                + "           TD.APP_ID AS APP_ID,"
                + "           TD.EVENT AS EVENT," 
                + "           TD.SUBTYPE AS SUBTYPE,"
                + "           TD.LABEL AS LABEL,"
                + "           TD.CUSTOM_VAL AS CUSTOM_VAL,"
                + "           '1' AS VISIT_COUNT,"
                + "           TD.TIME AS VISIT_TIME,"
                + "           TD.DURATION AS VISIT_DURATION,"
                + "           '' AS FROM_ID"//不需要FROM_ID
                + "     FROM  TMP_TOTAL_DATA TD ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_CALCULATE_VISIT_INFO",DataFrameUtil.CACHETABLE_PARQUET);
    }
    
    private DataFrame getStatisticsEventInfoResult(HiveContext sqlContext) {//最后去重，同一最早VISIT_TIME只能有一条数据
        String hql = "SELECT  *"
                + "     FROM  TMP_CALCULATE_VISIT_INFO"
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
                + "           FROM_ID";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FACT_STATISTICS_EVENT");
    }

    public JavaRDD<FactStatisticsEvent> getJavaRDD(DataFrame df) {
        RDD<Row> rdd = df.select("USER_ID", "APP_TYPE", "APP_ID", "EVENT", "SUBTYPE", "LABEL", "CUSTOM_VAL",
                "VISIT_COUNT", "VISIT_TIME", "VISIT_DURATION", "FROM_ID").rdd();//为避免代码冗余，与通用解析结果共同一个类
        JavaRDD<Row> jRDD = rdd.toJavaRDD();

        JavaRDD<FactStatisticsEvent> pRDD = jRDD.map(new Function<Row, FactStatisticsEvent>() {
            private static final long serialVersionUID = -2370716651591622510L;

            public FactStatisticsEvent call(Row v1) throws Exception {
                String rowKey = "";
                rowKey = IDUtil.getUUID().toString();
                return new FactStatisticsEvent(rowKey, v1.getString(0), v1.getString(1), v1.getString(2),
                        v1.getString(3), v1.getString(4), v1.getString(5), v1.getString(6),
                        v1.getString(7), v1.getString(8), v1.getString(9),v1.getString(10));
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
