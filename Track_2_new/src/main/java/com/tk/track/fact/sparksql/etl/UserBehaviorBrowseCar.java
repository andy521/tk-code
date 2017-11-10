package com.tk.track.fact.sparksql.etl;

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
 * @ClassName: UserBehaviorBrowseCar 车险
 * @Description: TODO
 * @author itw_wangcy
 * @date
 */
public class UserBehaviorBrowseCar {

	public DataFrame getUserBehaviorCarCuleDF(HiveContext sqlContext,String appids) {
		if (appids == null || appids.equals("")) {
			return null;
		}
		sqlContext.load(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH)).registerTempTable("FACT_CAR_EVENT");
		getCarBehaviorCuleDF1(sqlContext, appids);
		getUserBehaviorCuleDF2(sqlContext);
        return getUserBehaviorCuleDF6(sqlContext);

	}
	
	/**
	 * 查出符合条件的用户行为
	 * @param sqlContext
	 * @param appids
	 * @return
	 */
	public DataFrame getCarBehaviorCuleDF1(HiveContext sqlContext, String appids) {
		long todayTime = getTodayTime(-1);
		String format = new SimpleDateFormat("yyyy-MM-dd").format(todayTime);
		String hql = "SELECT * " + "FROM FACT_CAR_EVENT TB1 "
				+ "WHERE LOWER(TB1.APP_ID) in (" + appids.toLowerCase() + ")"
				+ "AND TB1.event ='车险报价'"
				+ " AND SUBSTR(FROM_UNIXTIME(INT(VISIT_TIME/1000)),1,10) ='"+ format+"'";
		
//		DataFrame df = DataFrameUtil.getDataFrame(sqlContext, hql,"TMP_FILTER_COLLECTION").distinct();
//		String path = "/user/tktest/taikangtrack/data/wcycar1";
//		if(TK_DataFormatConvertUtil.isExistsPath(path)){
//			TK_DataFormatConvertUtil.deletePath(path);
//		}
//		df.save(path);
//		return df;
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
     
//        DataFrame df = DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_USER_BEHAVIOR_CLUE").distinct();
//        String path = "/user/tktest/taikangtrack/data/wcycar1";
//        if(TK_DataFormatConvertUtil.isExistsPath(path)){
//        	TK_DataFormatConvertUtil.deletePath(path);
//        }
//        df.save(path);
//        return df;
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_USER_BEHAVIOR_CLUE").distinct();
    }
	 
	    /**
	     * 存入clue中
	     * @param sqlContext
	     * @return
	     */
	    public DataFrame getUserBehaviorCuleDF6(HiveContext sqlContext) {
	        String hql = "SELECT T5.ROWKEY,"
	        		+"       T5.USER_ID,   "
	        		+"       T5.USER_TYPE, "
	        		+"       T5.APP_TYPE,  "
	        		+"       T5.APP_ID,    "
	        		+"       'load' AS EVENT_TYPE,  "
	        		+"       T5.EVENT,     "
	        		+"       T5.SUBTYPE AS SUB_TYPE,"
	        		+"       '查询' AS PAGE_TYPE,   "
	        		+"       '车险' AS FIRST_LEVEL, "
	        		+"       '车险客户行为' AS SECOND_LEVEL, "
	        		+"       CASE          "
	        		+"         WHEN T5.APP_TYPE = 'wechat' THEN"
	        		+"          '微信车险报价页'    "
	        		+"      "
	        		+"         WHEN T5.APP_TYPE = 'app' THEN "
	        		+"          'APP车险报价页'     "
	        		+"      "
	        		+"         WHEN T5.APP_TYPE = 'H5' THEN  "
	        		+"          'WAP车险报价页'     "
	        		+"         ElSE        "
	        		+"          'PC车险报价页'      "
	        		+"       END AS THIRD_LEVEL,    "
	        		+"       '车险' AS FOURTH_LEVEL,"
	        		+"       T5.USER_NAME, "
	        		+"       T5.VISIT_COUNT,        "
	        		+"       FROM_UNIXTIME(INT(T5.VISIT_TIME / 1000)) AS VISIT_TIME,      "
	        		+"       T5.VISIT_DURATION,     "
	        		+"       T5.FROM_ID,   "
	        		+"       CONCAT('姓名：',       "
	        		+"              NVL(T5.USER_NAME, ' '),  "
	        		+"              '\073性别：',   "
	        		+"              NVL(T5.GENDER, ' '),     "
	        		+"              '\073出生日期：',        "
	        		+"              NVL(T5.BIRTHDAY, ' '),   "
	        		+"              '\073首次访问时间：',    "
	        		+"              NVL(FROM_UNIXTIME(INT(T5.VISIT_TIME / 1000)), ' '),"
	        		+"              '\073访问次数：',        "
	        		+"              NVL(T5.VISIT_COUNT, ' '),"
	        		+"              '\073访问时长：',        "
	        		+"              NVL(T5.VISIT_DURATION, ' ')) AS REMARK,"
	        		+"       '2' AS CLUE_TYPE       "
	        		+"  FROM TMP_USER_BEHAVIOR_CLUE T5";
	      
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
	    	JavaRDD<Row> jRDD = df.select("ROWKEY","USER_ID","USER_TYPE","APP_TYPE","APP_ID","EVENT_TYPE","EVENT","SUB_TYPE","VISIT_DURATION","FROM_ID","USER_NAME","PAGE_TYPE","FIRST_LEVEL","SECOND_LEVEL","THIRD_LEVEL","FOURTH_LEVEL",
	    			"VISIT_TIME","VISIT_COUNT","CLUE_TYPE","REMARK").rdd().toJavaRDD();
	        JavaRDD<FactUserBehaviorClue> pRDD = jRDD.map(new Function<Row,FactUserBehaviorClue>() {

				private static final long serialVersionUID = -1619903863589066084L;

				public FactUserBehaviorClue call(Row v1) throws Exception {
	                return new FactUserBehaviorClue(v1.getString(0), v1.getString(1), v1.getString(2), 
	                        v1.getString(3), v1.getString(4), v1.getString(5), 
	                        v1.getString(6), v1.getString(7), v1.getString(8), 
	                        v1.getString(9), v1.getString(10), v1.getString(11), 
	                        v1.getString(12), v1.getString(13), v1.getString(14), v1.getString(15),v1.getString(16),v1.getString(17),v1.getString(18),v1.getString(19));
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
