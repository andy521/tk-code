/**
 * @Title: TerminalMap.java
 * @Package: com.tk.track.fact.sparksql.etl
 * @Description: TODO
 * @author zhang.shy
 * @Data 2016年5月7日
 * @version 1.0
 */
package com.tk.track.fact.sparksql.etl;
import java.io.Serializable;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;
/**
 * @ClassName: TerminalMapApp
 * @Description: TODO
 * @author liran
 * @date 2016年8月19日
 */
public class TerminalMapApp implements Serializable {

    private static final long serialVersionUID = -8855146171916772404L;
    public DataFrame getTerminalMapInfoResultDF(HiveContext sqlContext) {
        getTerminalMapDF(sqlContext);
        getTerminalMapInfoSum(sqlContext);
        getTerminalMapInfoNoNull(sqlContext);
        getTerminalMapInfoNull(sqlContext);
        getTerminalMapInfoJoin(sqlContext);
        getTerminalMapInfoUnion(sqlContext);
        return getTerminalMapInfoResult(sqlContext);
    }

    private DataFrame getTerminalMapDF(HiveContext sqlContext) {
        String hql = "SELECT A.APP_TYPE,"
        		+ "          A.APP_ID," 
        		+ "          A.TERMINAL_ID," 
                + "          CASE WHEN A.USER_ID = '' OR A.USER_ID IS NULL THEN A.USER_ID"
                + "				  WHEN LOWER(A.USER_ID) LIKE '%undefined%' THEN ''"
                + "				  ELSE TRIM(GENDECODE(A.USER_ID,'UTF-8')) "
                + "			 END AS USER_ID," 
                + "          CASE WHEN A.CLIENTTIME = '' OR A.CLIENTTIME IS NULL THEN A.TIME"
                + "				  ELSE A.CLIENTTIME"
                + "			 END AS CLIENTTIME"
                + "     FROM TMP_UBA_LOG_EVENT A"
                + "    WHERE A.TERMINAL_ID <>'' "
                + "      AND A.TERMINAL_ID IS NOT NULL "
                + "      AND A.TERMINAL_ID NOT LIKE 'TK%' "
                + "		 AND A.APP_TYPE = 'app'";//只限app
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_TERMINAL_MAP_A",DataFrameUtil.CACHETABLE_PARQUET);
    }

    private DataFrame getTerminalMapInfoSum(HiveContext sqlContext) {
        String hql = "SELECT TM.APP_TYPE,"
        		+ "			 TM.APP_ID,"
        		+ "			 TM.TERMINAL_ID,"
                + "          TM.USER_ID,"
                + "          CAST(COUNT(TM.USER_ID) AS STRING) AS VISITCOUNT,"
                + "          CAST(MAX(CAST(TM.CLIENTTIME AS BIGINT)) AS STRING) AS LASTTIME"
                + "     FROM TMP_TERMINAL_MAP_A TM"
                + "    GROUP BY TM.APP_TYPE, TM.APP_ID, TM.TERMINAL_ID, TM.USER_ID";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_TERMINAL_MAP_G",DataFrameUtil.CACHETABLE_PARQUET);
    }
    
    private DataFrame getTerminalMapInfoNoNull(HiveContext sqlContext) {
        String hql = "SELECT FTM.APP_TYPE,"
        		+ "			 FTM.APP_ID,"
        		+ "			 FTM.TERMINAL_ID,"
                + "          FTM.USER_ID,"
                + "          FTM.VISITCOUNT,"
                + "          FTM.LASTTIME"
                + "     FROM TMP_TERMINAL_MAP_G FTM"
                + "    WHERE FTM.USER_ID IS NOT NULL"
                + "      AND FTM.USER_ID <> ''";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FACT_TERMINAL_MAP_NONULL",DataFrameUtil.CACHETABLE_EAGER);
    }
    
    private DataFrame getTerminalMapInfoNull(HiveContext sqlContext) {
        String hql = "SELECT FTM.APP_TYPE,"
        		+ "			 FTM.APP_ID,"
        		+ "			 FTM.TERMINAL_ID,"
                + "          FTM.USER_ID,"
                + "          FTM.VISITCOUNT,"
                + "          FTM.LASTTIME"
                + "     FROM TMP_TERMINAL_MAP_G FTM"
                + "    WHERE FTM.USER_ID IS NULL"
                + "       OR FTM.USER_ID = ''";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FACT_TERMINAL_MAP_NULL",DataFrameUtil.CACHETABLE_PARQUET);
    }
    
    private DataFrame getTerminalMapInfoJoin(HiveContext sqlContext) {
        String hql = " SELECT /*+MAPJOIN(FTM)*/"//数据量改变后需要更改
        		+ "           FTM.APP_TYPE,"
        		+ "			  FTM.APP_ID,"
        		+ "			  FTM.TERMINAL_ID,"
                + "           FTM.USER_ID,"
                + "           FTM.VISITCOUNT,"
                + "           FTM.LASTTIME,"
                + "           FU.MOBILE,"
                + "           FU.NAME"
                + "      FROM TMP_FACT_TERMINAL_MAP_NONULL FTM "
                + " LEFT JOIN (SELECT DISTINCT "
                + "					  MOBILE,"
                + "				      NAME,"
                + "					  MEMBER_ID"
                + "				FROM  FACT_USERINFO"
                + "			   WHERE  MEMBER_ID <> ''"
                + "				 AND  MEMBER_ID IS NOT NULL) FU"
                + "        ON FTM.USER_ID = FU.MEMBER_ID";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FACT_TERMINAL_MAP_JOIN",DataFrameUtil.CACHETABLE_PARQUET);
    }
    
    private DataFrame getTerminalMapInfoUnion(HiveContext sqlContext) {
        String hql = "SELECT A.APP_TYPE,"
        		+ "			 A.APP_ID,"
        		+ "			 A.TERMINAL_ID,"
                + "          A.USER_ID,"
                + "          A.VISITCOUNT,"
                + "          A.LASTTIME,"
                + "          A.MOBILE,"
                + "          A.NAME"
                + "     FROM TMP_FACT_TERMINAL_MAP_JOIN A"
                + "   UNION ALL"
                + "   SELECT B.APP_TYPE,"
        		+ "			 B.APP_ID,"
        		+ "			 B.TERMINAL_ID,"
                + "          B.USER_ID,"
                + "          B.VISITCOUNT,"
                + "          B.LASTTIME,"
                + "          '' AS MOBILE,"
                + "          '' AS NAME"
                + "     FROM TMP_FACT_TERMINAL_MAP_NULL B";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FACT_TERMINAL_MAP_APP");
    }

    private DataFrame getTerminalMapInfoResult(HiveContext sqlContext) {
        String hql = "SELECT TMA.APP_TYPE,"
        		+ "			 TMA.APP_ID,"
        		+ "			 TMA.TERMINAL_ID,"
                + "          TMA.USER_ID,"
                + "          CAST(COUNT(TMA.USER_ID) AS STRING) AS VISITCOUNT,"
                + "          CAST(MAX(CAST(TMA.LASTTIME AS BIGINT)) AS STRING) AS LASTTIME,"
                + "          CAST(MAX(CAST(TMA.MOBILE AS BIGINT)) AS STRING) AS MOBILE,"
                + "          CAST(MAX(TMA.NAME) AS STRING) AS NAME"
                + "     FROM TMP_FACT_TERMINAL_MAP_APP TMA"
                + "    GROUP BY TMA.APP_TYPE, TMA.APP_ID, TMA.TERMINAL_ID, TMA.USER_ID";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_TERMINAL_MAP_APP");
    }
    
    public void saveAsParquet(DataFrame df, String path) {
		TK_DataFormatConvertUtil.deletePath(path);
		df.saveAsParquetFile(path);
	}
}
