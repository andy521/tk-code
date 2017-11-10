/**
 *<p>File Name:WechatFreeCheck.java</p>
 *<p>Description:</p>
 *<p>Company:Taikang online</p>
 *<p>@author itw_huangjl</p>
 *<p>@date 2017年6月8日 上午10:56:59</p>
 */
package com.tk.track.fact.sparksql.etl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.util.DataFrameUtil;

public class UserBehaviorWechatFreeCheck {
	
	public DataFrame getWechatFreeCheckCuleDF(HiveContext sqlContext, String appids) {
		if (appids == null || appids.equals("")) {
			return null;
		}
		sqlContext.load(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH)).registerTempTable("WECHAT_FREECHECK_EVENT");
		getWechatFreeCheckCuleDF1(sqlContext, appids);
		getWechatFreeCheckCuleDF2(sqlContext);
		return getUserBehaviorCuleDF6(sqlContext);

	}

	/**
	 * 查出符合条件的用户行为
	 * 
	 * @param sqlContext
	 * @param appids
	 * @return
	 */
	public DataFrame getWechatFreeCheckCuleDF1(HiveContext sqlContext, String appids) {
		long todayTime = getTodayTime(-1);
		String format = new SimpleDateFormat("yyyy-MM-dd").format(todayTime);
		String hql = "SELECT * \r\n" + 
				"FROM WECHAT_FREECHECK_EVENT TB1 \r\n" + 
				" WHERE LOWER(TB1.APP_ID) = ("+appids+")\r\n" + 
				" AND SUBSTR(FROM_UNIXTIME(INT(VISIT_TIME/1000)),1,10) ='"+ format+"'";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FILTER_FREECHECK");

	}

	/**
	 * 查出用户基本信息
	 * 
	 * @param sqlContext
	 * @return
	 */
	public DataFrame getWechatFreeCheckCuleDF2(HiveContext sqlContext) {
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
	                + "     FROM TMP_FILTER_FREECHECK TMP_A "
	                + "     INNER JOIN (SELECT DISTINCT NAME,GENDER,BIRTHDAY,CUSTOMER_ID,MEMBER_ID,OPEN_ID "
	                + "					FROM FACT_USERINFO WHERE OPEN_ID IS NOT NULL AND OPEN_ID <> '') UM" 
	                + "       ON TMP_A.USER_ID = UM.OPEN_ID "
	                + "    WHERE TMP_A.APP_TYPE = 'wechat'"
	                + "   UNION ALL "
	                + "		SELECT TMP_A.ROWKEY, "
	            	+ "          TMP_A.FROM_ID, "
	        		+ "          TMP_A.APP_TYPE, "
	        		+ "          TMP_A.APP_ID, "
	        		+ "          TMP_A.USER_ID,"
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
	                + "     FROM TMP_FILTER_FREECHECK TMP_A "
	                + "     INNER JOIN (SELECT DISTINCT NAME,GENDER,BIRTHDAY,CUSTOMER_ID,MEMBER_ID,OPEN_ID "
	                + "					FROM FACT_USERINFO WHERE MEMBER_ID IS NOT NULL AND MEMBER_ID <> '') UM " 
	                + "       ON TMP_A.USER_ID = UM.MEMBER_ID "
	                + "    WHERE TMP_A.APP_TYPE <> 'wechat'";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_USER_BEHAVIOR_CLUE");
	}

	/**
	 * 存入clue中
	 * 
	 * @param sqlContext
	 * @return
	 */
	public DataFrame getUserBehaviorCuleDF6(HiveContext sqlContext) {
		String hql = "SELECT DISTINCT T5.ROWKEY,\r\n" +
				"       T5.USER_ID,\r\n" + 
				"       T5.USER_TYPE,\r\n" + 
				"       T5.APP_TYPE,\r\n" + 
				"       T5.APP_ID,\r\n" + 
				"       'load' AS EVENT_TYPE,\r\n" + 
				"       T5.EVENT,\r\n" + 
				"       T5.SUBTYPE AS SUB_TYPE,\r\n" + 
				"       '查询' AS PAGE_TYPE,\r\n" + 
				"       '微信经营活动' AS FIRST_LEVEL,\r\n" + 
				"       '日常活动' AS SECOND_LEVEL,\r\n" + 
				"       '父亲节免费领体检' AS THIRD_LEVEL,\r\n" + 
				"       '父亲节免费领体检' AS FOURTH_LEVEL,\r\n" + 
				"       T5.USER_NAME,\r\n" + 
				"       T5.VISIT_COUNT,\r\n" + 
				"       FROM_UNIXTIME(INT(T5.VISIT_TIME / 1000)) AS VISIT_TIME,\r\n" + 
				"       T5.VISIT_DURATION,\r\n" + 
				"       '63068' as FROM_ID,\r\n" + 
				"       case \r\n" + 
				"         when T5.SUBTYPE = '手机号' or T5.SUBTYPE = '立即领取' then \r\n" + 
				"           CONCAT('姓名：', NVL(T5.USER_NAME, ' '),\r\n" + 
				"                  '性别：', NVL(T5.GENDER, ' '), \r\n" + 
				"                  '出生日期：', NVL(T5.BIRTHDAY, ' '), \r\n" + 
				"                  '手机号：', NVL(T5.LABEL, ' '), \r\n" + 
				"                  '首次访问时间：', NVL(FROM_UNIXTIME(INT(T5.VISIT_TIME / 1000)), ' '), \r\n" + 
				"                  '访问次数：', NVL(T5.VISIT_COUNT, ' '), \r\n" + 
				"                  '访问时长：', NVL(T5.VISIT_DURATION, ' ')) \r\n" + 
				"          else \r\n" + 
				"           CONCAT('姓名：', NVL(T5.USER_NAME, ' '), \r\n" + 
				"                  '性别：', NVL(T5.GENDER, ' '), \r\n" + 
				"                  '出生日期：', NVL(T5.BIRTHDAY, ' '), \r\n" + 
				"                  '首次访问时间：', NVL(FROM_UNIXTIME(INT(T5.VISIT_TIME / 1000)), ' '), \r\n" + 
				"                  '访问次数：', NVL(T5.VISIT_COUNT, ' '), \r\n" + 
				"                  '访问时长：', NVL(T5.VISIT_DURATION, ' ')) \r\n" + 
				"        end AS REMARK,\r\n" + 
				"       '2' AS CLUE_TYPE\r\n" + 
				"  FROM TMP_USER_BEHAVIOR_CLUE T5";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_USER_BEHAVIOR_ClUE");
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
}
