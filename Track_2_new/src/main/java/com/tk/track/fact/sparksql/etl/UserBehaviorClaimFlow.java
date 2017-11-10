package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.fact.sparksql.util.DataFrameUtil;


/**
 * 通用理赔流程ETL处理
 * 
 * @author itw_qiuzq01
 *
 */
public class UserBehaviorClaimFlow implements Serializable {

	private static final long serialVersionUID = -4188185782425131924L;

	public DataFrame getUserBehaviorClaimFlowDF(HiveContext sqlContext, String appids) {
		if (null == appids || appids.equals("")) {
			return null;
		}

		getUserBehaviorClaimFlowFse(sqlContext,appids);
		
		getFactUserHaveOpenId(sqlContext);
		
		getFseWithUserinfo(sqlContext);

		getCfgClaimFlowRnk(sqlContext);
		
		getFlowFse(sqlContext);
		
		getFlowFseMaxRnk(sqlContext);
		
		getFlowFsePath(sqlContext);
		
		getSearchFse(sqlContext);
		
		return getClaimAll(sqlContext);
	}

	
	/**
	 * 获取通用理赔流程基础数据
	 * @Description: 
	 * @param sqlContext
	 * @param appids
	 * @return
	 * @author itw_qiuzq01
	 * @date 2016年9月26日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getUserBehaviorClaimFlowFse(HiveContext sqlContext, String appids) {
		String nowTimeStamp = Long.toString(getTodayTime(0) / 1000);
		String yesterdayTimeStamp = Long.toString(getTodayTime(-1) / 1000);

		String hql = "SELECT TRIM(TB1.USER_ID) USER_ID"
				+"      ,TB1.APP_ID INFOFROM"
				+"      ,TB1.SUBTYPE"
				+"      ,TB1.EVENT USER_EVENT"
				+"      ,TB1.VISIT_COUNT"
				+"      ,FROM_UNIXTIME(BIGINT(BIGINT(VISIT_TIME) / 1000),'yyyy-MM-dd HH:mm:ss') VISIT_TIME"
				+"      ,TB1.VISIT_DURATION"
				+"      ,TB1.FROM_ID"
				+"  FROM F_STATISTICS_EVENT TB1"
				+" WHERE LOWER(TB1.APP_TYPE) = 'wechat' AND LOWER(TB1.APP_ID) IN ("+appids.toLowerCase()+")";
		
//		if (TK_DataFormatConvertUtil.isExistsPath(TK_CommonConfig
//				.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORCLAIMFLOW_OUTPUTPATH))) {
			hql += " AND EVENT <> 'page.load' AND EVENT <> 'page.unload' "
					+ " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') >= from_unixtime(cast("
					+ yesterdayTimeStamp
					+ " as bigint),'yyyy-MM-dd HH:mm:ss')"
					+ " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') < from_unixtime(cast("
					+ nowTimeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')";
//		}
		return DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_F_STATISTICS_EVENT_CF");
	}
	
	
	
	/**
	 * 从FACT_USERINFO中获取微信号非空的记录
	 * @Description: 
	 * @param sqlContext
	 * @return
	 * @author itw_qiuzq01
	 * @date 2016年9月26日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getFactUserHaveOpenId(HiveContext sqlContext) {
		String hql = "SELECT DISTINCT AA.OPEN_ID,AA.MEMBER_ID,AA.NAME,AA.BIRTHDAY,AA.GENDER"
				+"  FROM (SELECT USER_ID, OPEN_ID, MEMBER_ID, NAME, BIRTHDAY, GENDER"
				+"          FROM FACT_USERINFO TFU"
				+"         WHERE OPEN_ID IS NOT NULL AND OPEN_ID <> '') AA"
				+"      ,(SELECT MAX(USER_ID) USER_ID, OPEN_ID"
				+"          FROM FACT_USERINFO TFU"
				+"         WHERE OPEN_ID IS NOT NULL AND OPEN_ID <> ''"
				+"         GROUP BY OPEN_ID) BB"
				+" WHERE AA.OPEN_ID = BB.OPEN_ID AND AA.USER_ID = BB.USER_ID";
		
		return DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_USER_HAVE_OPENID");
	}
	
	
	/**
	 * 事件数据关联报案人用户信息
	 * @Description: 
	 * @param sqlContext
	 * @return
	 * @author itw_qiuzq01
	 * @date 2016年9月26日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getFseWithUserinfo(HiveContext sqlContext) {
		String hql = "SELECT FSE.USER_ID USER_ID"
				+"      ,CASE"
				+"         WHEN U.MEMBER_ID IS NOT NULL THEN"
				+"          'MEM'"
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
				+"      ,FSE.INFOFROM"
				+"      ,CASE WHEN FSE.USER_EVENT LIKE '我要理赔%' THEN 'FW' ELSE 'SEARCH' END DTYPE"
				+"      ,FSE.SUBTYPE"
				+"      ,FSE.USER_EVENT"
				+"      ,FSE.VISIT_COUNT"
				+"      ,FSE.VISIT_TIME"
				+"      ,FSE.FROM_ID"
				+"  FROM TMP_F_STATISTICS_EVENT_CF FSE"
				+"  LEFT JOIN TMP_USER_HAVE_OPENID U ON FSE.USER_ID = TRIM(U.OPEN_ID)";
		
		return DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_Fse_With_Userinfo",DataFrameUtil.CACHETABLE_PARQUET);
	}
	
	

	/**
	 * 理赔流程序号配置表
	 * @Description: 
	 * @param sqlContext
	 * @return
	 * @author itw_qiuzq01
	 * @date 2016年9月28日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getCfgClaimFlowRnk(HiveContext sqlContext) {
		String hql = "SELECT '我要理赔-引导页首页' AS EVENT,'1' AS RNK UNION ALL "
				+"SELECT '我要理赔-填写出险信息首页' AS EVENT,'2' AS RNK UNION ALL "
				+"SELECT '我要理赔-储蓄卡首页' AS EVENT,'3' AS RNK UNION ALL "
				+"SELECT '我要理赔-支付宝首页' AS EVENT,'4' AS RNK UNION ALL "
				+"SELECT '我要理赔-身份证首页' AS EVENT,'5' AS RNK UNION ALL "
				+"SELECT '我要理赔-上传理赔资料首页' AS EVENT,'6' AS RNK UNION ALL "
				+"SELECT '我要理赔-理赔完成首页' AS EVENT,'7' AS RNK";
		
		return DataFrameUtil.getDataFrame(sqlContext, hql, "cfg_claim_flow_rnk",DataFrameUtil.CACHETABLE_PARQUET);
	}
	
	
	
	/**
	 * 获取理赔流程数据
	 * @Description: 
	 * @param sqlContext
	 * @return
	 * @author itw_qiuzq01
	 * @date 2016年9月26日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getFlowFse(HiveContext sqlContext) {
		String hql = "SELECT FWU.USER_ID"
				+"      ,FWU.USER_TYPE"
				+"      ,FWU.USER_NAME"
				+"      ,FWU.BIRTH"
				+"      ,FWU.GENDER"
				+"      ,FWU.INFOFROM"
				+"      ,FWU.DTYPE"
				+"      ,FWU.USER_EVENT"
				+"      ,FWU.VISIT_COUNT"
				+"      ,FWU.VISIT_TIME"
				+"      ,FWU.FROM_ID"
				+"      ,RK.RNK"
				+"  FROM TMP_FSE_WITH_USERINFO FWU"
				+" INNER JOIN CFG_CLAIM_FLOW_RNK RK ON FWU.USER_EVENT = RK.EVENT"
				+" WHERE FWU.DTYPE = 'FW'";
		
		return DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_Fse_Flow",DataFrameUtil.CACHETABLE_PARQUET);
	}
	
		
	
	
	/**
	 * 获取流程最大步骤
	 * @Description: 
	 * @param sqlContext
	 * @return
	 * @author itw_qiuzq01
	 * @date 2016年9月26日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getFlowFseMaxRnk(HiveContext sqlContext) {
		String hql = "SELECT TF.USER_ID, TF.FROM_ID, SUBSTR(TF.VISIT_TIME, 1, 10) VISIT_DAY, MAX(TF.RNK) MAX_RNK"
				+"  FROM TMP_FSE_FLOW TF"
				+" GROUP BY TF.USER_ID, TF.FROM_ID,SUBSTR(TF.VISIT_TIME, 1, 10)";
		
		return DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_Fse_Flow_Maxrnk");
	}
	
	
	
	
	/**
	 * 获取流程路径
	 * @Description: 
	 * @param sqlContext
	 * @return
	 * @author itw_qiuzq01
	 * @date 2016年9月26日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getFlowFsePath(HiveContext sqlContext) {
		String hql = "SELECT FWU.USER_ID"
				+"      ,FWU.USER_TYPE"
				+"      ,FWU.USER_NAME"
				+"      ,FWU.FROM_ID"
				+"      ,FWU.BIRTH"
				+"      ,FWU.GENDER"
				+"      ,FWU.INFOFROM"
				+"      ,FWU.USER_EVENT"
				+"      ,CONCAT('我要理赔-引导页首页->',FWU.USER_EVENT) LONGEST_PATH"
				+"      ,STRING(SUM(FWU.VISIT_COUNT)) VISIT_COUNT"
				+"      ,STRING(MIN(FWU.VISIT_TIME)) VISIT_TIME"
				+"      ,'流程' THIRD_LEVEL"
				+"      ,'' FOURTH_LEVEL"
				+"  FROM TMP_FSE_FLOW FWU"
				+" INNER JOIN TMP_FSE_FLOW_MAXRNK MR ON FWU.USER_ID = MR.USER_ID AND"
				+"                                      FWU.FROM_ID = MR.FROM_ID AND"
				+"                                      SUBSTR(FWU.VISIT_TIME, 1, 10) = MR.VISIT_DAY AND"
				+"                                      FWU.RNK = MR.MAX_RNK"
				+" GROUP BY FWU.USER_ID"
				+"         ,FWU.USER_TYPE"
				+"         ,FWU.USER_NAME"
				+"         ,FWU.FROM_ID"
				+"         ,FWU.BIRTH"
				+"         ,FWU.GENDER"
				+"         ,FWU.INFOFROM"
				+"         ,FWU.USER_EVENT";
		
		return DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_Fse_Flow_path");
	}
	
	
	/**
	 * 获取非流程相关的事件
	 * @Description: 
	 * @param sqlContext
	 * @return
	 * @author itw_qiuzq01
	 * @date 2016年9月27日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getSearchFse(HiveContext sqlContext) {
		String hql = "SELECT FWU.USER_ID"
				+"      ,FWU.USER_TYPE"
				+"      ,FWU.USER_NAME"
				+"      ,FWU.FROM_ID"
				+"      ,FWU.BIRTH"
				+"      ,FWU.GENDER"
				+"      ,FWU.INFOFROM"
				+"      ,FWU.SUBTYPE"
				+"      ,FWU.USER_EVENT"
				+"      ,'' LONGEST_PATH"
				+"      ,STRING(SUM(FWU.VISIT_COUNT)) VISIT_COUNT"
				+"      ,STRING(MIN(FWU.VISIT_TIME)) VISIT_TIME"
				+"      ,'查询' THIRD_LEVEL"
				+"      ,'' FOURTH_LEVEL"
				+"  FROM TMP_FSE_WITH_USERINFO FWU"
				+" WHERE FWU.DTYPE = 'SEARCH'"
				+" GROUP BY FWU.USER_ID"
				+"         ,FWU.USER_TYPE"
				+"         ,FWU.USER_NAME"
				+"         ,FWU.FROM_ID"
				+"         ,FWU.BIRTH"
				+"         ,FWU.GENDER"
				+"         ,FWU.INFOFROM"
				+"         ,FWU.SUBTYPE"
				+"         ,FWU.USER_EVENT";
		
		return DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_Fse_Search");
	}
	
	
	
	/**
	 * 获取流程与查询合并后的数据
	 * @Description: 
	 * @param sqlContext
	 * @return
	 * @author itw_qiuzq01
	 * @date 2016年9月26日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getClaimAll(HiveContext sqlContext) {
		String hql = "SELECT USER_ID"
				+"      ,USER_TYPE"
				+"      ,USER_NAME"
				+"      ,FROM_ID"
				+"      ,BIRTH"
				+"      ,GENDER"
				+"      ,INFOFROM"
				+"      ,'' SUBTYPE"
				+"      ,USER_EVENT"
				+"      ,LONGEST_PATH"
				+"      ,VISIT_COUNT"
				+"      ,VISIT_TIME"
				+"      ,THIRD_LEVEL"
				+"      ,FOURTH_LEVEL"
				+"  FROM TMP_FSE_FLOW_PATH"
				+" UNION ALL "
				+"SELECT USER_ID"
				+"      ,USER_TYPE"
				+"      ,USER_NAME"
				+"      ,FROM_ID"
				+"      ,BIRTH"
				+"      ,GENDER"
				+"      ,INFOFROM"
				+"      ,SUBTYPE"
				+"      ,USER_EVENT"
				+"      ,LONGEST_PATH"
				+"      ,VISIT_COUNT"
				+"      ,VISIT_TIME"
				+"      ,THIRD_LEVEL"
				+"      ,FOURTH_LEVEL"
				+"  FROM TMP_FSE_SEARCH";
		
		return DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_ClaimAll");
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
