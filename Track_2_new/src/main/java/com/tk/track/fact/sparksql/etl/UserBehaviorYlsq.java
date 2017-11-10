package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;


/**
 * 养老社区ETL处理
 * 
 * @author itw_qiuzq01
 *
 */
public class UserBehaviorYlsq implements Serializable {

	private static final long serialVersionUID = -4188185782425131924L;
	private static final Boolean isDebug = false;

	public DataFrame getUserBehaviorYlsqDF(HiveContext sqlContext, String appids) {
		if (null == appids || appids.equals("")) {
			return null;
		}

		getUserBehaviorYlsqFSE_1(sqlContext, appids);
		getMemberInfo_2(sqlContext);
		getUserBehaviorYlsqSummary_3(sqlContext);
		return getUserBehaviorYlsq(sqlContext);
	}

	/**
	 * 读取F_STATISTICS_EVENT中养老社区数据
	 * 
	 * @Description:
	 * @param sqlContext
	 * @param appids
	 * @return
	 * @author itw_qiuzq01
	 * @date 2016年9月12日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getUserBehaviorYlsqFSE_1(HiveContext sqlContext, String appids) {
		
		String nowTimeStamp = Long.toString(getTodayTime(0) / 1000);
		String yesterdayTimeStamp = Long.toString(getTodayTime(-1) / 1000);
		
		String hql = "SELECT TRIM(TB1.USER_ID) USER_ID"
				+ "      ,TB1.APP_ID"
				+ "      ,TB1.SUBTYPE"
				+ "      ,TB1.EVENT USER_EVENT"
				+ "      ,TB1.VISIT_COUNT"
				+ "      ,TB1.VISIT_TIME"
				+ "      ,TB1.VISIT_DURATION"
				+ "      ,TB1.FROM_ID"
				+ "  FROM F_STATISTICS_EVENT TB1"
				+ " WHERE LOWER(TB1.APP_TYPE) = 'h5' AND LOWER(TB1.APP_ID) IN (" + appids.toLowerCase() + ")" ;
		if (TK_DataFormatConvertUtil.isExistsPath(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORTELE_OUTPUTPATH))) {
				hql += " AND EVENT <> 'page.load' AND EVENT <> 'page.unload' "
				+ " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') >= from_unixtime(cast(" + yesterdayTimeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')"
	    		+ " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') < from_unixtime(cast(" + nowTimeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')";
		}
		DataFrame df = null;

		if (isDebug) {
			System.out.println("-------------getUserBehaviorYlsqFSE_1-------------------");
			System.out.println(hql);
			System.out.println("-------------getUserBehaviorYlsqFSE_1-------------------");
		} else {
//			df = DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_F_STATISTICS_EVENT_ylsq",DataFrameUtil.CACHETABLE_PARQUET);
			df = DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_F_STATISTICS_EVENT_ylsq");
		}
		
		return df;
		
	}

	/**
	 * 从p_member中获取会员信息
	 * 
	 * @Description:
	 * @param sqlContext
	 * @return
	 * @author itw_qiuzq01
	 * @date 2016年9月12日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getMemberInfo_2(HiveContext sqlContext) {
		String hql = "SELECT DISTINCT TFU.MEMBER_REALNAME NAME"
				+"      ,TRIM(TFU.MEMBER_ID) MEMBER_ID"
				+"      ,SUBSTR(TFU.MEMBER_BIRTH,1,10) BIRTHDAY"
				+"      ,TFU.MEMBER_GENDER GENDER"
				+"  FROM P_MEMBER TFU";
		
		DataFrame df = null;

		if (isDebug) {
			System.out.println("-------------getFactUserinfo_2-------------------");
			System.out.println(hql);
			System.out.println("-------------getFactUserinfo_2-------------------");
		} else {
//			df =  DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_MemberInfo",DataFrameUtil.CACHETABLE_PARQUET);
			df =  DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_MemberInfo");
		}
		
		return df;
	}

	/**
	 * 用户行为与用户信息表关联，获取会员性别生日等信息
	 * @Description: 
	 * @param sqlContext
	 * @return
	 * @author itw_qiuzq01
	 * @date 2016年9月13日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private  DataFrame getUserBehaviorYlsqSummary_3(HiveContext sqlContext) {
		String hql = "SELECT  "
//				+"		/*+MAPJOIN(YL)*/" 
				+ " 	'' ROWKEY"
				+"      ,YL.USER_ID"
				+"      ,'MEM' USER_TYPE"
				+"      ,YL.USER_EVENT"
				+"      ,'养老社区预约' THIRD_LEVEL"
				+"      ,'养老社区预约' FOURTH_LEVEL"
				+"      ,'' LRT_ID"
				+"      ,'' CLASS_ID"
				+"      ,YL.APP_ID INFOFROM"
				+"      ,SUM(VISIT_COUNT) VISIT_COUNT"
				+"      ,FROM_UNIXTIME(BIGINT(BIGINT(MIN(VISIT_TIME)) / 1000),'yyyy-MM-dd HH:mm:ss') VISIT_TIME"
				+"      ,SUM(VISIT_DURATION) VISIT_DURATION"
				+"      ,'' CLASSIFY_NAME"
				+"      ,'' PRODUCT_NAME"
				+"      ,'1' IF_PAY"
				+"      ,YL.FROM_ID FROM_ID"
				+"      ,COALESCE(U.NAME,'') USER_NAME"
				+"      ,COALESCE(U.BIRTHDAY,'') BIRTHDAY"
				+"      ,CASE"
				+"         WHEN U.GENDER = '0' THEN"
				+"          '男'"
				+"         WHEN U.GENDER = '1' THEN"
				+"          '女'"
				+"         ELSE"
				+"          '其他'"
				+"       END GENDER"
				+"  FROM TMP_F_STATISTICS_EVENT_YLSQ YL"
				+"  LEFT JOIN Tmp_MemberInfo U ON YL.USER_ID = U.MEMBER_ID"
				+" WHERE YL.USER_EVENT IN ('养老社区') AND YL.SUBTYPE = '提交'"
				+" GROUP BY YL.USER_ID"
				+"         ,YL.USER_EVENT"
				+"         ,YL.APP_ID"
				+"         ,YL.FROM_ID"
				+"         ,COALESCE(U.NAME,'')"
				+"         ,COALESCE(U.BIRTHDAY,'')"
				+"         ,CASE"
				+"            WHEN U.GENDER = '0' THEN"
				+"             '男'"
				+"            WHEN U.GENDER = '1' THEN"
				+"             '女'"
				+"            ELSE"
				+"             '其他'"
				+"          END"
				+" "
				+"UNION ALL"
				+" "
				+"SELECT  "
//				+"		/*+MAPJOIN(YL)*/" 
				+ " 	'' ROWKEY"
				+"      ,YL.USER_ID"
				+"      ,'MEM' USER_TYPE"
				+"      ,YL.USER_EVENT"
				+"      ,'养老社区' THIRD_LEVEL"
				+"      ,CONCAT(CASE"
				+"                WHEN TRIM(YL.FROM_ID) IN ('53685', '54325') THEN"
				+"                 'App'"
				+"                ELSE"
				+"                 'Wap'"
				+"              END"
				+"             ,'养老社区') FOURTH_LEVEL"
				+"      ,'' LRT_ID"
				+"      ,'' CLASS_ID"
				+"      ,YL.APP_ID INFOFROM"
				+"      ,SUM(VISIT_COUNT) VISIT_COUNT"
				+"      ,FROM_UNIXTIME(BIGINT(BIGINT(MIN(VISIT_TIME)) / 1000),'yyyy-MM-dd HH:mm:ss') VISIT_TIME"
				+"      ,SUM(VISIT_DURATION) VISIT_DURATION"
				+"      ,'' CLASSIFY_NAME"
				+"      ,'' PRODUCT_NAME"
				+"      ,'1' IF_PAY"
				+"      ,YL.FROM_ID FROM_ID"
				+"      ,COALESCE(U.NAME,'') USER_NAME"
				+"      ,COALESCE(U.BIRTHDAY,'') BIRTHDAY"
				+"      ,CASE"
				+"         WHEN U.GENDER = '0' THEN"
				+"          '男'"
				+"         WHEN U.GENDER = '1' THEN"
				+"          '女'"
				+"         ELSE"
				+"          '其他'"
				+"       END GENDER"
				+"  FROM TMP_F_STATISTICS_EVENT_YLSQ YL"
				+"  LEFT JOIN Tmp_MemberInfo U ON YL.USER_ID = U.MEMBER_ID"
				+" WHERE (YL.USER_EVENT IN ('养老社区') AND YL.SUBTYPE <> '提交') OR YL.USER_EVENT NOT IN ('养老社区')"
				+" GROUP BY YL.USER_ID"
				+"         ,YL.USER_EVENT"
				+"         ,YL.APP_ID"
				+"         ,YL.FROM_ID"
				+"         ,COALESCE(U.NAME,'')"
				+"         ,COALESCE(U.BIRTHDAY,'')"
				+"         ,CASE"
				+"            WHEN U.GENDER = '0' THEN"
				+"             '男'"
				+"            WHEN U.GENDER = '1' THEN"
				+"             '女'"
				+"            ELSE"
				+"             '其他'"
				+"          END";
		
		DataFrame df = null;

		if (isDebug) {
			System.out.println("-------------getUserBehaviorYlsqSummary_3-------------------");
			System.out.println(hql);
			System.out.println("-------------getUserBehaviorYlsqSummary_3-------------------");

		} else {
//			df =  DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_Ylsq_summary",DataFrameUtil.CACHETABLE_PARQUET);
			df =  DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_Ylsq_summary");
		}
		
		return df;
		

	}
	
	/**
	 * 获取养老社区线索数据
	 * 
	 * @Description:
	 * @param sqlContext
	 * @return
	 * @author itw_qiuzq01
	 * @date 2016年9月12日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getUserBehaviorYlsq(HiveContext sqlContext) {
		String hql = "SELECT ROWKEY"
				+"      ,USER_ID"
				+"      ,USER_TYPE"
				+"      ,USER_EVENT"
				+"      ,THIRD_LEVEL"
				+"      ,FOURTH_LEVEL"
				+"      ,LRT_ID"
				+"      ,CLASS_ID"
				+"      ,INFOFROM"
				+"      ,STRING(VISIT_COUNT) VISIT_COUNT"
				+"      ,VISIT_TIME"
				+"      ,STRING(VISIT_DURATION) VISIT_DURATION"
				+"      ,CLASSIFY_NAME"
				+"      ,PRODUCT_NAME"
				+"      ,USER_NAME"
				+"      ,IF_PAY"
				+"      ,FROM_ID"
				+"      ,CASE"
				+"         WHEN YS.THIRD_LEVEL = '养老社区预约' THEN"
				+"          CONCAT('访问页面:预约养老社区;姓名:', YS.USER_NAME)"
				+"         WHEN YS.THIRD_LEVEL = '养老社区' THEN"
				+"          CONCAT('访问页面:',YS.FOURTH_LEVEL,'页面;姓名:'"
				+"                ,YS.USER_NAME"
				+"                ,';性别:'"
				+"                ,YS.GENDER"
				+"                ,';出生日期:'"
				+"                ,YS.BIRTHDAY"
				+"                ,';会员登录名:'"
				+"                ,YS.USER_ID"
				+"                ,';当天首次访问时间:'"
				+"                ,YS.VISIT_TIME"
				+"                ,';当天访问该页面次数:'"
				+"                ,YS.VISIT_COUNT"
				+"                ,';当天停留该页面总时长:'"
				+"                ,mseconds2hms(YS.VISIT_DURATION)"
				+"                )"
				+"       END REMARK,"
				+ "		 '' AS CLUE_TYPE"
				+"  FROM TMP_YLSQ_SUMMARY YS";
		
		DataFrame df = null;

		if (isDebug) {
			System.out.println("-------------getUserBehaviorYlsq-------------------");
			System.out.println(hql);
			System.out.println("-------------getUserBehaviorYlsq-------------------");
		} else {
			df =  DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_USER_BEHAVIOR_TELE");
		}
		
		return df;
		
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
	
	/**
	 * @Description: 
	 * @param args
	 * @author itw_qiuzq01
	 * @date 2016年9月12日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	public static void main(String[] args) {
		long nowdate = getTodayTime(0);
		long yestoday = getTodayTime(-1);
		System.out.println("now===="+ nowdate);
		System.out.println("yes===="+yestoday); 
		
	}
	

}
