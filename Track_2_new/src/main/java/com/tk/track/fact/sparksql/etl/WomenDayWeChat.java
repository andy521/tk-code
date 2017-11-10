package com.tk.track.fact.sparksql.etl;

import com.tk.track.fact.sparksql.util.DataFrameUtil;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import java.io.Serializable;


public class WomenDayWeChat implements Serializable {

	
	private static final long serialVersionUID = -8437963687669525344L;

	public DataFrame 	getWomenDayWeChatDF(HiveContext sqlContext) {
		getFStaticEventTmp(sqlContext);//.show(10);
		getUserInfo(sqlContext);//.show(10);
		getTmpWomenDayWechat(sqlContext).show(10);
		DataFrame df  = getWomenDayWechat(sqlContext);
		df.show(10);
		return df;
	}

	private DataFrame getFStaticEventTmp(HiveContext sqlContext) {
    	String hql = "SELECT  USER_ID AS OPEN_ID,"
    			+ "			  APP_TYPE,"
    			+ "			  APP_ID,"
    			+ "			  EVENT,"
    			+ "			  SUBTYPE,"
    			+ "			  LABEL,"
    			+ "			  VISIT_COUNT,"
    			+ "			  VISIT_TIME,"
    			+ "			  VISIT_DURATION,"
    			+ "			  FROM_ID"
                + " FROM  F_STATISTICS_EVENT " 
                + " WHERE APP_TYPE = 'wechat' AND APP_ID = 'wechat065'"
                + " AND USER_ID IS NOT NULL AND USER_ID <> ''";
    	return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_WOMENFS");
    }
	
	
	private DataFrame getUserInfo(HiveContext sqlContext) {	
		String hql =  "SELECT HUSER.NAME, SUBSTR(HUSER.BIRTHDAY,0,10) AS BIRTHDAY, " +
						"	 CASE"+
						"         WHEN HUSER.GENDER = '0' THEN"+
						"          '男'"+
						"         WHEN HUSER.GENDER = '1' THEN"+
						"          '女'"+
						"         ELSE"+
						"          '其他'"+
						"       END AS GENDER,\n"+
						"HUSER.OPEN_ID,\n"+
						"CASE WHEN HUSER.CUSTOMER_ID IS NOT NULL AND HUSER.CUSTOMER_ID <> '' THEN HUSER.CUSTOMER_ID\n" +
						"WHEN HUSER.MEMBER_ID IS NOT NULL AND HUSER.MEMBER_ID <> '' THEN HUSER.MEMBER_ID\n" +
						"ELSE HUSER.OPEN_ID END AS USER_ID,\n" +
						"CASE WHEN HUSER.CUSTOMER_ID IS NOT NULL AND HUSER.CUSTOMER_ID <> '' THEN 'C'\n" +
						"WHEN HUSER.MEMBER_ID IS NOT NULL AND HUSER.MEMBER_ID <> '' THEN 'MEM'\n" +
						"ELSE 'WE' END AS USER_TYPE\n" +
						"FROM FACT_USERINFO HUSER \n" +
						" INNER JOIN (SELECT OPEN_ID, MAX(USER_ID) USER_ID\n"+
						"               FROM FACT_USERINFO\n"+
						"              WHERE OPEN_ID IS NOT NULL AND OPEN_ID <> ''\n"+
						"              GROUP BY OPEN_ID) MAX_USER ON HUSER.USER_ID = MAX_USER.USER_ID\n"+
						" WHERE HUSER.OPEN_ID IS NOT NULL AND HUSER.OPEN_ID <> ''";
			
			return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_WOMENDAYUSERINFO");
	}

	public DataFrame getTmpWomenDayWechat(HiveContext sqlContext) {
		
		String hql = "SELECT "
				+ "	HUSER.USER_ID,"
				+ " HUSER.USER_TYPE,"
				+ "	FS.APP_TYPE,"
				+ "	FS.APP_ID,"
				+ "	FS.EVENT,"
				+ "	FS.SUBTYPE AS SUB_TYPE,"
				+ "	FS.VISIT_DURATION,"
				+ "	FS.FROM_ID,"
				+ " HUSER.NAME AS USER_NAME,"
				+ " HUSER.GENDER,"
				+ " HUSER.BIRTHDAY,"
				+ "	FROM_UNIXTIME(INT(FS.VISIT_TIME / 1000)) AS VISIT_TIME,"
				+ " FS.VISIT_COUNT,"
				+ "	CONCAT('姓名：',"
				+ " 	NVL(NAME,' '),"
				+ " 	'\073性别：',"
				+ " 	NVL(GENDER,' '),"
				+ " 	'\073出生日期：',"
				+ " 	NVL(BIRTHDAY,' '),"
				+ " 	'\073首次访问时间：',"
				+ " 	NVL(FROM_UNIXTIME(INT(FS.VISIT_TIME / 1000)),' '),"
				+ " 	'\073访问次数：',"
				+ " 	NVL(VISIT_COUNT,' '),"
				+ " 	'\073访问时长：',"
				+ " 	NVL(VISIT_DURATION,' ')," +
				"      '\073活动名称：','3.8节活动') AS REMARK"
				+ " FROM TMP_WOMENFS FS"
				+ " INNER JOIN TMP_WOMENDAYUSERINFO HUSER ON FS.OPEN_ID = HUSER.OPEN_ID";
				
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_WOMENWECHAT");
	}
	
	public DataFrame getWomenDayWechat(HiveContext sqlContext) {
		String hql = "SELECT DISTINCT '' AS ROWKEY,"
				+ "	USER_ID,"
				+ "	USER_TYPE,"
				+ "	APP_TYPE,"
				+ "	APP_ID,"
				+ " '微信三八节活动' AS EVENT_TYPE,"
				+ "	EVENT,"
				+ "	SUB_TYPE,"
				+ "	VISIT_DURATION,"
				+ "	FROM_ID,"
				+ "	'微信三八节活动' AS PAGE_TYPE,"
				+ "	'大健康营销活动' AS FIRST_LEVEL,"
				+ "	'微信下行三八节活动' AS SECOND_LEVEL,"
				+ "	'三八节活动参与客户' AS THIRD_LEVEL,"
				+ " '微信三八节活动' AS FOURTH_LEVEL,"
				+ "	VISIT_TIME,"
				+ " VISIT_COUNT,"
				+ " '2' AS CLUE_TYPE,"
				+ "	REMARK,"
				+ " USER_NAME"
				+ " FROM TMP_WOMENWECHAT";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_WOMENWECHAT");
		
	}
}
