package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;



/**
 * 通用理赔流程ETL处理
 * 
 * @author itw_qiuzq01
 *
 */
public class UserBehaviorWechatHealthTest implements Serializable {

	private static final long serialVersionUID = -4188185782425131924L;
	
	String sca_ub = TK_DataFormatConvertUtil.getUserBehaviorSchema();
	
	
	public DataFrame getSrcDataWithUserInfoDF(HiveContext sqlContext, String appids) {
		if (null == appids || appids.equals("")) {
			return null;
		}

		getSrcData(sqlContext,appids);
		
		
		getLastestTestInfo(sqlContext);
		
		getUserInfo(sqlContext);

		return getSrcDataWithUserInfo(sqlContext);
				
	}
	
	public DataFrame getUserBehaviorWechatHealthTestDF(HiveContext sqlContext) {
		
		getTmpWechatHealthTestResult(sqlContext);
		
		return getWechatHealthTestResult(sqlContext);
		
	}

	/*
	 * 获取最新用户行为数据
	 * 
	 */
	
	
	private DataFrame getSrcData(HiveContext sqlContext, String appids) {
		String nowTimeStamp = Long.toString(getTodayTime(0) / 1000);
		String yesterdayTimeStamp = Long.toString(getTodayTime(-1) / 1000);

		String hql = "SELECT FS.USER_ID AS OPEN_ID"
				+"      ,FS.APP_ID"
				+"      ,FS.EVENT"
				+"      ,FS.SUBTYPE"
				+"      ,FS.VISIT_COUNT"
				+"      ,FS.VISIT_TIME"
				+"      ,FS.VISIT_DURATION"
				+"      ,FS.FROM_ID"
				+"  FROM F_STATISTICS_EVENT FS"
				+" WHERE LOWER(FS.APP_TYPE) = 'wechat' AND"
				+"       LOWER(FS.APP_ID) IN ("+appids.toLowerCase()+") AND FS.USER_ID IS NOT NULL AND"
				+"       FS.USER_ID <> '' AND FS.EVENT IS NOT NULL AND FS.EVENT <> '' AND FS.EVENT NOT LIKE 'page%'";
		
		
//		if (TK_DataFormatConvertUtil.isExistsPath(TK_CommonConfig
//				.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORHEALTHTEST_OUTPUTPATH))) {
			hql += " AND  from_unixtime(cast(cast(FS.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') >= from_unixtime(cast("
					+ yesterdayTimeStamp
					+ " as bigint),'yyyy-MM-dd HH:mm:ss')"
					+ " AND  from_unixtime(cast(cast(FS.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') < from_unixtime(cast("
					+ nowTimeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')";
//		}
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_SRC_DATA");
	}
	
	
	/*
	 * 获取最新用户测试结果数据
	 * 
	 */
	
	private DataFrame getLastestTestInfo(HiveContext sqlContext) {
		String hql = "SELECT DISTINCT FH.OPEN_ID"
				+"		, FH.TEST_SORT"
				+"		,CASE"
				+"		   WHEN FH.TEST_SORT = '10' THEN"
				+"			CONCAT('寿命:',REGEXP_EXTRACT(FH.TEST_NAME,'(\\\\d+.\\\\d+)(.+?\"ageBeatRate)',1))"
				+"		   WHEN FH.TEST_SORT = '11' THEN"
				+"			CONCAT('儿童身高:',REGEXP_EXTRACT(FH.TEST_NAME,'(\\\\d+.\\\\d+)(.+?\"childShapeEval)',1))"
				+"		   WHEN FH.TEST_SORT = '12' THEN"
				+"			CONCAT('乳腺癌:',REGEXP_EXTRACT(FH.TEST_NAME,'(?<=r\":).+(?=\\,\"absLifetime)',0))"
				+"		   WHEN FH.TEST_SORT = '20' THEN"
				+"		    CONCAT('抑郁程度:',REGEXP_EXTRACT(FH.TEST_NAME,'\\\\d+.\\\\d+',0))"
				+"		   ELSE"
				+"			FH.TEST_NAME"
				+"		 END AS TEST_NAME"
				+"  FROM (SELECT HT.OPEN_ID, HT.TEST_SORT, MAX(HT.SUBMIT_DATE) MAX_SUBMIT_DATE"
				+"			FROM FACT_HEALTHTESTSCORE HT"
				+"         WHERE HT.OPEN_ID IS NOT NULL AND HT.OPEN_ID <> ''"
				+"         GROUP BY HT.OPEN_ID, HT.TEST_SORT) MAX_T"
				+" INNER JOIN FACT_HEALTHTESTSCORE FH ON MAX_T.OPEN_ID = FH.OPEN_ID AND MAX_T.TEST_SORT = FH.TEST_SORT "
				+"       AND MAX_T.MAX_SUBMIT_DATE = FH.SUBMIT_DATE"
				+" WHERE FH.OPEN_ID IS NOT NULL AND FH.OPEN_ID <> ''";
		
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_LASTEST_TEST_INFO");
	}
	
	/*
	 * 获取测试用户基本信息
	 * 
	 */
	
	
	private DataFrame getUserInfo(HiveContext sqlContext) {	
		
		String hql = "SELECT HUSER.OPEN_ID, HUSER.MEMBER_ID, HUSER.CUSTOMER_ID, HUSER.NAME, HUSER.BIRTHDAY, HUSER.GENDER"
				+"  FROM FACT_USERINFO HUSER"
				+" INNER JOIN (SELECT OPEN_ID, MAX(USER_ID) USER_ID"
				+"               FROM FACT_USERINFO"
				+"              WHERE OPEN_ID IS NOT NULL AND OPEN_ID <> ''"
				+"              GROUP BY OPEN_ID) MAX_USER ON HUSER.USER_ID = MAX_USER.USER_ID"
				+" WHERE HUSER.OPEN_ID IS NOT NULL AND HUSER.OPEN_ID <> ''";
		
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_USERINFO");
	}
	
	/*
	 * 关联 最新用户行为数据表 和 最新用户测试结果表
	 * 
	 */
	

	private DataFrame getSrcDataWithUserInfo(HiveContext sqlContext) {
		String hql = "SELECT /*+MAPJOIN(CFG)*/ SRC.OPEN_ID"
				+"		,CASE"
				+"		   WHEN HUSER.MEMBER_ID IS NOT NULL AND HUSER.MEMBER_ID <> '' THEN"
				+"			HUSER.MEMBER_ID"
				+"		   WHEN HUSER.CUSTOMER_ID IS NOT NULL AND HUSER.CUSTOMER_ID <> '' THEN"
				+"			HUSER.CUSTOMER_ID"
				+"		   ELSE"
				+"			SRC.OPEN_ID"
				+"		 END AS USER_ID"
				+"      ,CASE"
				+"         WHEN HUSER.MEMBER_ID IS NOT NULL AND HUSER.MEMBER_ID <> '' THEN"
				+"          'MEM'"
				+"         WHEN HUSER.CUSTOMER_ID IS NOT NULL AND HUSER.CUSTOMER_ID <> '' THEN"
				+"          'C'"
				+"		   ELSE"
				+"			'WE'"
				+"       END AS USER_TYPE"
				+"      ,HUSER.NAME"
				+"      ,HUSER.BIRTHDAY"
				+"      ,CASE"
				+"         WHEN HUSER.GENDER = '0' THEN"
				+"          '男'"
				+"         WHEN HUSER.GENDER = '1' THEN"
				+"          '女'"
				+"         ELSE"
				+"          '其他'"
				+"       END AS GENDER"
				+"      ,SRC.EVENT"
				+"      ,SRC.APP_ID"
				+"      ,SRC.SUBTYPE"
				+"      ,SRC.VISIT_COUNT"
				+"      ,FROM_UNIXTIME(INT(SRC.VISIT_TIME / 1000)) VISIT_TIME"
				+"      ,SRC.VISIT_DURATION"
				+"      ,TI.TEST_NAME LATEST_TEST_RESULT"
				+"      ,SRC.FROM_ID"
				+"  FROM TMP_SRC_DATA SRC"
				+" INNER JOIN TMP_USERINFO HUSER ON SRC.OPEN_ID = HUSER.OPEN_ID"
				+"  LEFT JOIN "+ sca_ub + "CFG_HEALTHTEST CFG ON SRC.EVENT = CFG.EVENT"
				+"  LEFT JOIN TMP_LASTEST_TEST_INFO TI ON SRC.OPEN_ID = TI.OPEN_ID AND CFG.TEST_SORT = TI.TEST_SORT";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_SRC_DATA_WITH_USERINFO");
	}
	
	
	
	private DataFrame getTmpWechatHealthTestResult(HiveContext sqlContext) {
		String hql = "SELECT USER_ID"
				+"      ,USER_TYPE"
				+"      ,APP_ID"
				+"      ,NAME AS USER_NAME"
				+"      ,BIRTHDAY AS BIRTH"
				+"      ,GENDER"
				+"      ,EVENT"
				+"      ,SUBTYPE AS SUB_TYPE"
				+"      ,STRING(SUM(VISIT_COUNT)) VISIT_COUNT"
				+"      ,STRING(MIN(VISIT_TIME)) VISIT_TIME"
				+"		,VISIT_DURATION"
				+"      ,LATEST_TEST_RESULT"
				+"      ,FROM_ID"
				+"  FROM TMP_SRC_DATA_WITH_USERINFO"
				+"	WHERE USER_TYPE IN ('MEM','C')"
				+" GROUP BY USER_ID"
				+"         ,USER_TYPE"
				+"         ,NAME"
				+"         ,BIRTHDAY"
				+"         ,GENDER"
				+"         ,EVENT"
				+"         ,APP_ID"
				+"         ,SUBTYPE"
				+"		   ,VISIT_DURATION"
				+"         ,LATEST_TEST_RESULT"
				+"         ,FROM_ID";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_WECHAT_HEALTHTEST");
	}
	
	
	/*
	 * 得到最终推送线索表
	 * 
	 */
	
	
	private DataFrame getWechatHealthTestResult(HiveContext sqlContext) {
		String hql = "SELECT '' ROWKEY,"
				+"       USER_ID,"
				+"       USER_TYPE,"
				+"       'wechat' APP_TYPE,"
				+"       APP_ID,"
				+"       'flow' EVENT_TYPE,"
				+"       EVENT,"
				+"       SUB_TYPE,"
				+"       VISIT_DURATION,"
				+"       FROM_ID,"
				+"       '微信测试' PAGE_TYPE,"
				+"       '微信经营活动' FIRST_LEVEL,"
				+"       '健康测试' SECOND_LEVEL,"
//				+"       '武汉营销数据' FIRST_LEVEL,"
//				+"       '微信健康测试数据' SECOND_LEVEL,"
				+"       '客户测试详情' THIRD_LEVEL,"
				+"       EVENT AS FOURTH_LEVEL,"
				+"       VISIT_TIME,"
				+"       VISIT_COUNT,"
				+"       '2' CLUE_TYPE,"
				+"       CONCAT('姓名：',"
				+"              NVL(USER_NAME,' '),"
				+"              '\073性别：',"
				+"              NVL(GENDER,' '),"
				+"              '\073出生日期：',"
				+"              NVL(BIRTH,' '),"
				+"              '\073首次访问时间：',"
				+"              NVL(VISIT_TIME,' '),"
				+"              '\073访问次数：',"
				+"              NVL(VISIT_COUNT,' '),"
				+"              '\073访问时长：',"
				+"              NVL(VISIT_DURATION,' '),"
				+"              '\073最新测试结果：',"
				+"              NVL(LATEST_TEST_RESULT,' ')) AS REMARK,"
				+"       USER_NAME"
				+"  FROM TMP_WECHAT_HEALTHTEST";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "WECHAT_HEALTHTEST_RESULT");
	}
	
	
	
	private static long getTodayTime(int day) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, day);
		String yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		long timeStamp = 0;
		try {
			timeStamp = sdf.parse(yesterday).getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return timeStamp;
	}
	
	
}
