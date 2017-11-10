package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.desttable.FactUserBehaviorClue;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

/**
 * @ClassName: UserBehaviorCountPremium e享健康保费测算线索
 * @Description: TODO
 * @author itw_wangzc01
 * @date
 */
public class UserBehaviorJulyActivity  implements Serializable{

	public DataFrame getUserBehaviorCarCuleDF(HiveContext sqlContext,String appids) {
		if (appids == null || appids.equals("")) {
			return null;
		}
		sqlContext.load(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH)).registerTempTable("FACT_WECHAT_EVENT");
		getUserBehaviorCuleDF1(sqlContext, appids);
		getUserBehaviorCuleDF2(sqlContext);
        return getUserBehaviorCuleDF3(sqlContext);

	}
	
	/**
	 * 查出符合条件的用户行为
	 * @param sqlContext
	 * @param appids
	 * @return
	 */
	public DataFrame getUserBehaviorCuleDF1(HiveContext sqlContext, String appids) {
		long todayTime = getTodayTime(-1);
		String format = new SimpleDateFormat("yyyy-MM-dd").format(todayTime);
		String hql = "SELECT * " + "FROM FACT_WECHAT_EVENT TB1 "
				+ "WHERE LOWER(TB1.APP_ID) = (" + appids.toLowerCase() + ")"
				+ " AND SUBSTR(FROM_UNIXTIME(INT(VISIT_TIME/1000)),1,10) ='"+ format+"'";
		return DataFrameUtil.getDataFrame(sqlContext, hql,"TMP_FILTER_COLLECTION").distinct();
	}

	

	/**
	 * 找打opendid 转话成memberid，未找到的存openid
	 * 查出用户的基本信息
	 * @param sqlContext
	 * @return
	 */
	public DataFrame getUserBehaviorCuleDF2(HiveContext sqlContext) {
        String hql = "SELECT TMP_A.ROWKEY, "
        		+ "          TMP_A.FROM_ID, "
        		+ "          TMP_A.APP_TYPE, "
        		+ "          TMP_A.APP_ID, "
        		+ "           CASE WHEN UM.MEMBER_ID IS NOT NULL AND UM.MEMBER_ID <> '' THEN  UM.MEMBER_ID"
        		+ "				WHEN UM.CUSTOMER_ID IS NOT NULL AND UM.CUSTOMER_ID <>'' THEN UM.CUSTOMER_ID "
        		+ "				ELSE TMP_A.USER_ID END USER_ID ,"
        		
        		+ "           CASE WHEN UM.MEMBER_ID IS NOT NULL AND UM.MEMBER_ID <> '' THEN  'MEM'"
        		+ "				WHEN UM.CUSTOMER_ID IS NOT NULL AND UM.CUSTOMER_ID <>'' THEN  'C' "
        		+ "				ELSE 'WE' END USER_TYPE ,"
                + "          TMP_A.EVENT, "
                + "          TMP_A.SUBTYPE,"
                + "          TMP_A.LABEL,"
                + "			 TMP_A.CUSTOM_VAL,"
                + "          TMP_A.VISIT_COUNT, "
                + "          TMP_A.VISIT_TIME, "
                + "          TMP_A.VISIT_DURATION,"
                + "			 UM.NAME AS USER_NAME,"
                + "			 CASE WHEN UM.GENDER = '0' THEN '男' WHEN  UM.GENDER ='1' THEN '女' ELSE '' END GENDER,"
                + "			 UM.BIRTHDAY"
                + "     FROM TMP_FILTER_COLLECTION TMP_A "
                + "     LEFT JOIN (SELECT NAME,GENDER,BIRTHDAY,CUSTOMER_ID,MEMBER_ID,OPEN_ID "
                + "					FROM FACT_USERINFO WHERE OPEN_ID IS NOT NULL AND OPEN_ID <> '') UM" 
                + "       ON TMP_A.USER_ID = UM.OPEN_ID "
                + "    WHERE TMP_A.APP_TYPE = 'wechat'"
                + "   UNION ALL "
                + "		SELECT TMP_A.ROWKEY, "
            	+ "          TMP_A.FROM_ID, "
        		+ "          TMP_A.APP_TYPE, "
        		+ "          TMP_A.APP_ID, "
        		+ "         TMP_A.USER_ID,"
                + "          'MEM' AS USER_TYPE, "
                + "          TMP_A.EVENT, "
                + "          TMP_A.SUBTYPE,"
                + "          TMP_A.LABEL,"
                + "			 TMP_A.CUSTOM_VAL,"
                + "          TMP_A.VISIT_COUNT, "
                + "          TMP_A.VISIT_TIME, "
                + "          TMP_A.VISIT_DURATION, "
                + "			 UM.NAME,"
                + "			 CASE WHEN UM.GENDER = '0' THEN '男' WHEN  UM.GENDER ='1' THEN '女' ELSE '' END GENDER,"
                + "			 UM.BIRTHDAY"
                + "     FROM TMP_FILTER_COLLECTION TMP_A "
                + "     LEFT JOIN (SELECT NAME,GENDER,BIRTHDAY,CUSTOMER_ID,MEMBER_ID,OPEN_ID "
                + "					FROM FACT_USERINFO WHERE MEMBER_ID IS NOT NULL AND MEMBER_ID <> '') UM " 
                + "       ON TMP_A.USER_ID = UM.MEMBER_ID "
                + "    WHERE TMP_A.APP_TYPE <> 'wechat'";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_USER_BEHAVIOR_CLUE").distinct();
    }
	    /**
	     * 结果存入clue中
	     * @param sqlContext
	     * @return
	     */
	    public DataFrame getUserBehaviorCuleDF3(HiveContext sqlContext) {
	        String hql = "SELECT T5.ROWKEY,"
	        		+"       T5.USER_ID,   "
	        		+"       T5.USER_TYPE, "
	        		+"       T5.APP_TYPE,  "
	        		+"       T5.APP_ID,    "
	        		+"       'load' AS EVENT_TYPE,  "
	        		+"       T5.EVENT,     "
	        		+"       T5.SUBTYPE AS SUB_TYPE,"
	        		+"       T5.VISIT_DURATION,"
	        		+"       '63068' AS FROM_ID,"
	        		+"       T5.USER_NAME, "
	        		+"       '查询' AS PAGE_TYPE,   "
			        +"       '微信经营活动' AS FIRST_LEVEL, "
			        +"       '日常活动' AS SECOND_LEVEL, "
			        +"       '7月满送活动' AS THIRD_LEVEL,    "
	        		+"       CASE          "
	        		+"         WHEN T5.EVENT IN ('活动产品-成人必备','活动产品-全部必备','活动产品-少儿必备','活动产品-孝心必备') THEN "
	        		+"          P.DPLANNAME    "
	        		+"         ElSE        "
	        		+"          '7月满送活动(浏览页面)' END AS FOURTH_LEVEL, "
/*	        		+"       CASE          "
	        		+"         WHEN T5.EVENT IN ('活动产品-成人必备','活动产品-全部必备','活动产品-少儿必备','活动产品-孝心必备') THEN "
	        		+"          P.RISKCODE "
	        		+"         ElSE        "
	        		+"          'T1051'  "
	        		+"       END AS FIFTH_LEVEL,    "*/
	        		+"       FROM_UNIXTIME(INT(T5.VISIT_TIME / 1000)) AS VISIT_TIME,      "
	        		+"       T5.VISIT_COUNT,        "
	        		+"       '2' AS CLUE_TYPE ,      "
	        		+"       CONCAT( '\073访问时长：',        "
	        		+"              NVL(T5.VISIT_DURATION, ' ')) AS REMARK"
	        		+"       FROM TMP_USER_BEHAVIOR_CLUE T5 "
	        		+"      LEFT JOIN TKUBDB.JULYACTIVITYPLANCODE P "
	        		+"       ON P.SUBTYPE =T5.SUBTYPE ";
	      
	        return DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_USER_BEHAVIOR_ClUE").distinct();
	    }
	    
	    public long getTodayTime(int day) {
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DATE, day);
			String yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			long timeStamp = 0;
			try {
				timeStamp = sdf.parse(yesterday).getTime();
			} catch (ParseException e) {
				e.printStackTrace();
			}
			return timeStamp;
		}
	    
	    public JavaRDD<FactUserBehaviorClue> getJavaRDD(DataFrame df) {
	    	JavaRDD<Row> jRDD = df.select("ROWKEY", "USER_ID", "USER_TYPE", "APP_TYPE", "APP_ID",
	                "EVENT_TYPE", "EVENT", "SUB_TYPE", "VISIT_DURATION", "FROM_ID", "PAGE_TYPE",
	                "FIRST_LEVEL", "SECOND_LEVEL", "THIRD_LEVEL", "FOURTH_LEVEL", "VISIT_TIME","VISIT_COUNT","CLUE_TYPE","REMARK","USER_NAME").rdd().toJavaRDD();
	    	
	        JavaRDD<FactUserBehaviorClue> pRDD = jRDD.map(new Function<Row, FactUserBehaviorClue>() {

	            private static final long serialVersionUID = -1619903863589066084L;

	            public FactUserBehaviorClue call(Row row) throws Exception {
	                String ROWKEY = row.getString(0);
	                String USER_ID = row.getString(1);
	                String USER_TYPE = row.getString(2);
	                String APP_TYPE = row.getString(3);
	                String APP_ID = row.getString(4);
	                String EVENT_TYPE = row.getString(5);
	                String EVENT = row.getString(6);
	                String SUB_TYPE = row.getString(7);
	                String VISIT_DURATION = row.getString(8);
	                String FROM_ID = row.getString(9);
	                String PAGE_TYPE = row.getString(10);
	                String FIRST_LEVEL = row.getString(11);
	                String SECOND_LEVEL = row.getString(12);
	                String THIRD_LEVEL = row.getString(13);
	                String FOURTH_LEVEL = row.getString(14);
	                String VISIT_TIME = row.getString(15);
	                String VISIT_COUNT = row.getString(16);
	                String CLUE_TYPE = row.getString(17);
	                String REMARK = row.getString(18);
	                String USER_NAME = row.getString(19);
	                return new FactUserBehaviorClue(ROWKEY, USER_ID, USER_TYPE,
	                        APP_TYPE, APP_ID, EVENT_TYPE, EVENT, SUB_TYPE,
	                        VISIT_DURATION, FROM_ID, PAGE_TYPE, FIRST_LEVEL,
	                        SECOND_LEVEL, THIRD_LEVEL, FOURTH_LEVEL, VISIT_TIME,
	                        VISIT_COUNT, CLUE_TYPE, REMARK, USER_NAME);
	            }
	        });
	        return pRDD;
	    }
	    
	    public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<FactUserBehaviorClue> rdd, String path) {
			if (TK_DataFormatConvertUtil.isExistsPath(path)) {
				sqlContext.createDataFrame(rdd, FactUserBehaviorClue.class).save(path, "parquet", SaveMode.Append);
			} else {
				sqlContext.createDataFrame(rdd, FactUserBehaviorClue.class).saveAsParquetFile(path);
			}
		}
	    
	    public void saveAsParquet(DataFrame df, String path) {
			TK_DataFormatConvertUtil.deletePath(path);
			df.saveAsParquetFile(path);
		}
}
