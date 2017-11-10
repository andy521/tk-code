package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.fact.sparksql.util.DMUtility;
import com.tk.track.fact.sparksql.util.DataFrameUtil;


public class WeChatActivity implements Serializable {

	
	private static final long serialVersionUID = -8437963687669525344L;

	public DataFrame 	getWeChatActivityDF(HiveContext sqlContext) {
		getFStaticEventTmp(sqlContext);//.show(10);
		getUserInfo(sqlContext);//.show(10);
		getWechatActivity(sqlContext).show(10);
		DataFrame df  = getFianlWechatAtInfo(sqlContext);
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
                + " WHERE APP_TYPE = 'wechat' AND APP_ID = '"+TK_CommonConfig.getValue("WeChatActivityCode")+"'"
                + " AND USER_ID IS NOT NULL AND USER_ID <> ''";
    	String path = "/user/tktest/taikangtrack/data/wechat_activity1";
    	DataFrame df = DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_USERS");
    	 DMUtility.deletePath(path);
    	 df.save(path);
    	return df;
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
		
		String path = "/user/tktest/taikangtrack/data/wechat_activity2";
    	DataFrame df = DataFrameUtil.getDataFrame(sqlContext, hql, "WCAC_USERINFO");
    	 DMUtility.deletePath(path);
    	 df.save(path);
    	return df;
	}

	public DataFrame getWechatActivity(HiveContext sqlContext) {
		
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
				"      '\073活动名称：','"+TK_CommonConfig.getValue("WeChatActivityName")+"') AS REMARK"
				+ " FROM TMP_USERS FS"
				+ " INNER JOIN WCAC_USERINFO HUSER ON FS.OPEN_ID = HUSER.OPEN_ID";
				
		String path = "/user/tktest/taikangtrack/data/wechat_activity3";
    	DataFrame df = DataFrameUtil.getDataFrame(sqlContext, hql, "WCAC_WECHAT");
    	 DMUtility.deletePath(path);
    	 df.save(path);
    	return df;
	}
	
	public DataFrame getFianlWechatAtInfo(HiveContext sqlContext) {
		String hql = "SELECT DISTINCT '' AS ROWKEY,"
				+ "	USER_ID,"
				+ "	USER_TYPE,"
				+ "	APP_TYPE,"
				+ "	APP_ID,"
				+ " '"+TK_CommonConfig.getValue("WeChatActivityName")+"' AS EVENT_TYPE,"
				+ "	EVENT,"
				+ "	SUB_TYPE,"
				+ "	VISIT_DURATION,"
				+ "	FROM_ID,"
				+ "	'"+TK_CommonConfig.getValue("WeChatActivityName")+"' AS PAGE_TYPE,"
				+ "	'"+TK_CommonConfig.getValue("WeChatActivityFirstlevel")+"' AS FIRST_LEVEL,"
				+ "	'"+TK_CommonConfig.getValue("WeChatActivitySecondlevel")+"' AS SECOND_LEVEL,"
				+ "	'"+TK_CommonConfig.getValue("WeChatActivityThirdlevel")+"' AS THIRD_LEVEL,"
				+ " '"+TK_CommonConfig.getValue("WeChatActivityFourthlevel")+"' AS FOURTH_LEVEL,"
				+ "	VISIT_TIME,"
				+ " VISIT_COUNT,"
				+ " '"+TK_CommonConfig.getValue("WeChatActivityCluetype")+"' AS CLUE_TYPE,"
				+ "	REMARK,"
				+ " USER_NAME"
				+ " FROM WCAC_WECHAT";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "WCAC_USERWECHAT");
		
	}
}
