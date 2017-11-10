package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.fact.sparksql.util.DataFrameUtil;

/**
 * 微信【我的】ETL处理
 * 
 * @author itw_qiuzq01
 *
 */
public class UserBehaviorWechatMy implements Serializable {

	private static final long serialVersionUID = 5530591264507850147L;

	public DataFrame getUserBehaviorWechatMyDF(HiveContext sqlContext) {

		getUserBehaviorWechatMyFse(sqlContext);
		getUserBehaviorUserHaveOpenId(sqlContext);
		getUserBehaviorUserLrtList(sqlContext);
		
		return getUserBehaviorFseWithUserLrtList(sqlContext);

	}

	/**
	 * 获取用户浏览基础数据
	 * 
	 * @Description:
	 * @param sqlContext
	 * @return
	 * @author itw_qiuzq01
	 * @date 2016年10月11日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getUserBehaviorWechatMyFse(HiveContext sqlContext) {
		String nowTimeStamp = Long.toString(getTodayTime(0) / 1000);
		String yesterdayTimeStamp = Long.toString(getTodayTime(-1) / 1000);
//		String yesterdayTimeStamp = Long.toString(getTodayTime(-6) / 1000);
		
		String hql = "SELECT TRIM(TB1.USER_ID) USER_ID" 
				+ "      ,TB1.APP_ID INFOFROM" 
				+ "      ,CASE WHEN TB1.SUBTYPE = 'sharetimeline' THEN '分享到朋友圈' WHEN  TB1.SUBTYPE = 'sharefriends' THEN '分享到朋友'  ELSE  TB1.SUBTYPE END SUBTYPE"
				+ "      ,CASE WHEN TB1.EVENT = 'share' THEN '分享' ELSE TB1.EVENT END USER_EVENT" 
				+ "      ,TB1.VISIT_COUNT"
				+ "      ,FROM_UNIXTIME(BIGINT(BIGINT(VISIT_TIME) / 1000),'yyyy-MM-dd HH:mm:ss') VISIT_TIME"
				+ "      ,TB1.VISIT_DURATION" 
				+ "      ,TB1.FROM_ID" 
				+ "  FROM F_STATISTICS_EVENT TB1"
				+ " WHERE LOWER(TB1.APP_TYPE) = 'wechat' AND LOWER(TB1.APP_ID) = 'wechat004'"
				+ " AND EVENT <> 'page.load' AND EVENT <> 'page.unload' "
				+ " AND EVENT <> '' AND EVENT IS NOT NULL "
				+ " AND EVENT NOT LIKE '%å%'";
//				+ " AND EVENT NOT IN ('å®Œå–„ä¸ªäººä¿¡æ�¯','æ³°å�¥åº·æœ�åŠ¡','è¯„åˆ†è§£è¯»','æ³°åº·åœ¨çº¿é¦–é¡µ','æ³°åº·åœ¨çº¿','ä½“å¥—é¤�æ£€','è¯„åˆ†è§£è¯»é¦–é¡µ') ";


//		if (TK_DataFormatConvertUtil.isExistsPath(TK_CommonConfig
//				.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORWECHATMY_OUTPUTPATH))) {
			hql += " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') >= from_unixtime(cast("
					+ yesterdayTimeStamp
					+ " as bigint),'yyyy-MM-dd HH:mm:ss')"
					+ " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') < from_unixtime(cast("
					+ nowTimeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')";
//		}

		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_F_STATISTICS_EVENT_WECHAT");
	}

	
	
	/**
	 * 获取微信号不为空的用户信息 同一个OPENID有多条记录的，取USERID最大的一条
	 * @Description:
	 * @param sqlContext
	 * @return
	 * @author itw_qiuzq01
	 * @date 2016年10月11日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getUserBehaviorUserHaveOpenId(HiveContext sqlContext) {

		String hql = "SELECT DISTINCT AA.OPEN_ID, AA.MEMBER_ID,AA.CUSTOMER_ID, AA.NAME, AA.BIRTHDAY, AA.GENDER"
				+ "  FROM (SELECT USER_ID, OPEN_ID, MEMBER_ID,CUSTOMER_ID , NAME, BIRTHDAY, GENDER"
				+ "          FROM FACT_USERINFO TFU"
				+ "         WHERE OPEN_ID IS NOT NULL AND OPEN_ID <> '') AA"
				+ "      ,(SELECT MAX(USER_ID) USER_ID, OPEN_ID"
				+ "          FROM FACT_USERINFO TFU"
				+ "         WHERE OPEN_ID IS NOT NULL AND OPEN_ID <> ''"
				+ "         GROUP BY OPEN_ID) BB"
				+ " WHERE AA.OPEN_ID = BB.OPEN_ID AND AA.USER_ID = BB.USER_ID";

		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_USER_HAVE_OPENID",DataFrameUtil.CACHETABLE_PARQUET);
	}

	
	/**
	 * 获取用户的历史保单、产品列表
	 * @Description: 
	 * @param sqlContext
	 * @return
	 * @author itw_qiuzq01
	 * @date 2016年10月11日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getUserBehaviorUserLrtList(HiveContext sqlContext) {

		String hql = "SELECT UHO.OPEN_ID"
				+ "      ,CONCAT_WS('||', COLLECT_SET(CONCAT(FNP.LRT_ID,'-', FNP.LRT_NAME))) LRTS"
				+ "      ,CONCAT_WS('||', COLLECT_SET(TRIM(FNP.LIA_POLICYNO))) POLICYS"
				+ "  FROM TMP_USER_HAVE_OPENID UHO"
				+ " INNER JOIN F_NET_POLICYSUMMARY FNP ON UHO.CUSTOMER_ID = FNP.POLICYHOLDER_ID"
				+ " GROUP BY UHO.OPEN_ID";

		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_USER_LRT_LIST");
	}
	
	
	/**
	 * 事件数据关联用户信息
	 * @Description: 
	 * @param sqlContext
	 * @return
	 * @author itw_qiuzq01
	 * @date 2016年10月11日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getUserBehaviorFseWithUserLrtList(HiveContext sqlContext) {

		String hql = "SELECT CASE"
				+"         WHEN U.MEMBER_ID IS NOT NULL AND U.MEMBER_ID <> '' THEN"
				+"          U.MEMBER_ID"
				+"         WHEN U.CUSTOMER_ID IS NOT NULL AND U.CUSTOMER_ID <> '' THEN"
				+"          U.CUSTOMER_ID"
				+"         ELSE"
				+"          FSE.USER_ID"
				+"       END USER_ID"
				+"      ,CASE"
				+"         WHEN U.MEMBER_ID IS NOT NULL AND U.MEMBER_ID <> '' THEN"
				+"          'MEM'"
				+"         WHEN U.CUSTOMER_ID IS NOT NULL AND U.CUSTOMER_ID <> '' THEN"
				+"          'C'"
				+"         ELSE"
				+"          'WE'"
				+"       END USER_TYPE"
				+"      ,U.NAME USER_NAME"
				+"      ,U.BIRTHDAY BIRTH"
				+"      ,CASE"
				+"         WHEN U.GENDER = '0' THEN"
				+"          '男'"
				+"         WHEN U.GENDER = '1' THEN"
				+"          '女'"
				+"         ELSE"
				+"          '其他'"
				+"       END GENDER"
				+"      ,FSE.USER_EVENT"
				+"      ,'' THIRD_LEVEL"
				+"      ,'' FOURTH_LEVEL"
				+"      ,FSE.INFOFROM"
				+"      ,FSE.SUBTYPE"
				+"      ,FSE.VISIT_COUNT"
				+"      ,FSE.VISIT_TIME"
				+"      ,FSE.VISIT_DURATION"
				+"      ,FSE.FROM_ID"
				+"      ,LRT.LRTS HIS_LRTS"
				+"      ,LRT.POLICYS HIS_POLICYNOS"
				+"  FROM TMP_F_STATISTICS_EVENT_WECHAT FSE"
				+"  LEFT JOIN TMP_USER_HAVE_OPENID U ON FSE.USER_ID = U.OPEN_ID"
				+"  LEFT JOIN TMP_USER_LRT_LIST LRT ON FSE.USER_ID = LRT.OPEN_ID";

		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FSE_WITH_USERINFO_LRTS");
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
