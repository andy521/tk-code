package com.tk.track.fact.sparksql.etl;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

/**
 * 
* <p>Title: 泰康大数据部</p>
* <p>Description: 通用投保流程线索表</p>
* <p>Copyright: Copyright (c) 2016</p>
* <p>Company: 大连华信有限公司</p>
* <p>Department: 总公司\数据信息中心\IT外包公司\大连华信总公司\数据信息中心\IT外包公司\大连华信部</p>
* @author moyunqing
* @date   2016年9月22日
* @version 1.0
 */
public class InsuranceProcessClue {
	private static final String sca_nw = TK_DataFormatConvertUtil.getNetWorkSchema();
	
	/**
	 * 
	 * @Description: 投保流程线索生成
	 * @param sqlContext
	 * @param config
	 * @param isall
	 * @return
	 * @author moyunqing
	 * @date 2016年9月29日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	public DataFrame getInsuranceProcessClue(HiveContext sqlContext,Map<String,String> config,boolean isall){
		getSrcData(sqlContext,isall);
		getAllProcessSccess(sqlContext,config);
		getAllProcessfailed(sqlContext,config);
		getAllclue(sqlContext);
		getUserNotNullMaxLocus(sqlContext);
		getUserNotNullMaxRecord(sqlContext);
		getUserNotNullInfo(sqlContext);
		getUserNullMaxLocus(sqlContext);
		getUserNullMaxRecord(sqlContext);
		getUserNullInfo(sqlContext);
		getNearSuccessProcess(sqlContext);
		getSuccessProcessInfo(sqlContext);
		getAllClueInfo(sqlContext);
		getCustomerinfo(sqlContext);
		getIsBuy(sqlContext);
		getTmpRelation(sqlContext);
		resolveRelation1(sqlContext);
		resolveRelation2(sqlContext);
		resolveRelation3(sqlContext);
		getFactInsuranceProcessClue(sqlContext);
		return getWhFactUserBehaviorClue(sqlContext);
	}
	
	/**
	 * 
	 * @Description: 加载线索中间表数据，并区分是否全量
	 * @param sqlContext
	 * @param isall
	 * @return
	 * @author moyunqing
	 * @date 2016年9月29日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getSrcData(HiveContext sqlContext, boolean isall) {
		String sql = "SELECT  *"
				+ "	   FROM   FACT_INSURANCEPROCESS_EVENT"
				+ "	  WHERE   ((USER_ID <> '' AND USER_ID IS NOT NULL)"
				+ "		   OR (MOBILE <> '' AND MOBILE IS NOT NULL))";
		if(!isall)
			sql += "	AND FROM_UNIXTIME(CAST(CAST(START_TIME AS BIGINT)/1000 AS BIGINT),'yyyy-MM-dd') = '" + getDate(-1) + "'";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_Src",DataFrameUtil.CACHETABLE_PARQUET);
	}

	/**
	 * 
	 * @Description: 缓存d_relation
	 * @param sqlContext
	 * @author moyunqing
	 * @date 2016年9月29日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getTmpRelation(HiveContext sqlContext) {
		String sql = "SELECT  *"
				+ "	   FROM    " + sca_nw + "D_RELATION ";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_rela",DataFrameUtil.CACHETABLE_EAGER);
	}

	/**
	 * 
	 * @Description: 对生成线索表
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月29日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getFactInsuranceProcessClue(HiveContext sqlContext) {
		String sql = "SELECT  DISTINCT"
				+ "			  APP_TYPE,"
				+ "			  APP_ID,"
				+ "			  TERMINAL_ID,"
				+ "			  USER_TYPE,"
				+ "			  USER_ID,"
				+ "			  STATUS,"
				+ "			  INTERRUPT_REASON,"
				+ "			  FORM_ID,"
				+ "			  LRT_ID,"
				+ "			  LRT_NAME,"
				+ "			  SALE_MODE,"
				+ "			  BABY_GENDER,"
				+ "			  BABY_BIRTHDAY,"
				+ "			  INSURE_TYPE,"
				+ "			  SEX,"
				+ "			  BABY_RELATION,"
				+ "			  P_BIRTHDAY,"
				+ "			  PAY_TYPE,"
				+ "			  PAY_PERIOD,"
				+ "			  PH_NAME,"
				+ "			  PH_BIRTHDAY,"
				+ "			  PH_EMAIL,"
				+ "			  PH_INDUSTRY,"
				+ "			  PH_WORKTYPE,"
				+ "			  INS_RELATION,"
				+ "			  INS_NAME,"
				+ "			  INS_BIRTHDAY,"
				+ "			  BENE_RELATION,"
				+ "			  BENE_NAME,"
				+ "			  BENE_BIRTHDAY,"
				+ "			  SUCCESS,"
				+ "			  LOCUS,"
				+ "			  LAST_STEP,"
				+ "			  START_TIME,"
				+ "			  END_TIME,"
				+ "			  FROM_ID,"
				+ "			  CLUE_TYPE,"
				+ "			  IS_CLUE,"
				+ "			  CAST(PROCESS_COUNT AS STRING) AS PROCESS_COUNT,"
				+ "			  USER_NAME,"
				+ "			  IS_BUY,"
				+ "			  MOBILE,"
				+ "			  '' AS FIRST_LEVEL,"
				+ "			  '' AS SECOND_LEVEL,"
				+ "			  '' AS THIRD_LEVEL,"
				+ "			  '' AS FOURTH_LEVEL,"
				+ "			  '' AS REMARK"
				+ "	   FROM   TMP_RSRL3";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Fact_InsuranceProcess_clue");
	}
	
	private DataFrame getWhFactUserBehaviorClue(HiveContext sqlContext) {
		String sql = "SELECT "
				+ "			  APP_TYPE,"
				+ "			  CONCAT('insuranceProcess_',APP_ID) AS APP_ID,"
				+ "			  USER_ID,"
				+ "			  USER_TYPE,"
				+ "			  '武汉营销数据' AS FIRST_LEVEL,"
				+ "			  '官网会员' AS SECOND_LEVEL,"
				+ "			  '通用投保获取' AS THIRD_LEVEL,"
				+ "			  '投保中断' AS FOURTH_LEVEL,"
				+ "			  '投保轨迹' AS EVENT,"
				+ "			  'flow' AS EVENT_TYPE,"
				+ "			  '' AS SUB_TYPE,"
				+ "			  '0' AS VISIT_DURATION,"
				+ "			  FROM_ID,"
				+ "           CASE WHEN IS_CLUE='1' THEN '1'"
			    + "           	   WHEN IS_CLUE='0' THEN '2'"
				+ "           END AS CLUE_TYPE,"
				+ "			  USER_NAME,"
				+ "			  '投保' AS PAGE_TYPE,"		
				+ "			  CAST(PROCESS_COUNT AS STRING) AS VISIT_COUNT,"
				+ "			  from_unixtime(cast(cast(START_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') AS VISIT_TIME,"
				+ "           CONCAT('姓名：',NVL(USER_NAME,''),'\073产品名称：',NVL(LRT_NAME,''), '\073是否为少儿险：',CASE WHEN NVL(CLUE_TYPE,'0')='1' THEN '是' WHEN NVL(CLUE_TYPE,'0')='0' THEN '否' END,'\073投保次数：',NVL(PROCESS_COUNT,'0'),'\073投保轨迹：',NVL(LOCUS,''),'\073投保单号：',NVL(FORM_ID,''),"
				+ "					 '\073投保状态：',CASE WHEN NVL(STATUS,'0')='1' THEN '成功' WHEN NVL(STATUS,'0')='0' THEN '失败' END, '\073投保结束时间：',from_unixtime(cast(cast(END_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss')) AS REMARK "
				+ "	   FROM   Fact_InsuranceProcess_clue";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "WH_FACT_USERBEHAVIOR_CLUE");
	}

	/**
	 * 
	 * @Description: 将受益人与被保险人关系转为中文字符
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月29日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame resolveRelation3(HiveContext sqlContext) {
		String sql = "SELECT  /*+MAPJOIN(DR)*/"
				+ "			  RL2.APP_TYPE,"
				+ "			  RL2.APP_ID,"
				+ "			  RL2.TERMINAL_ID,"
				+ "			  RL2.USER_TYPE,"
				+ "			  RL2.USER_ID,"
				+ "			  RL2.STATUS,"
				+ "			  RL2.INTERRUPT_REASON,"
				+ "			  RL2.FORM_ID,"
				+ "			  RL2.LRT_ID,"
				+ "			  RL2.LRT_NAME,"
				+ "			  RL2.SALE_MODE,"
				+ "			  RL2.BABY_GENDER,"
				+ "			  RL2.BABY_BIRTHDAY,"
				+ "			  RL2.INSURE_TYPE,"
				+ "			  RL2.SEX,"
				+ "			  RL2.BABY_RELATION,"
				+ "			  RL2.P_BIRTHDAY,"
				+ "			  RL2.PAY_TYPE,"
				+ "			  RL2.PAY_PERIOD,"
				+ "			  RL2.PH_NAME,"
				+ "			  RL2.PH_BIRTHDAY,"
				+ "			  RL2.PH_EMAIL,"
				+ "			  RL2.PH_INDUSTRY,"
				+ "			  RL2.PH_WORKTYPE,"
				+ "			  RL2.INS_RELATION,"
				+ "			  RL2.INS_NAME,"
				+ "			  RL2.INS_BIRTHDAY,"
				+ "			  CASE WHEN DR.RE_NAME <> '' AND DR.RE_NAME IS NOT NULL THEN DR.RE_NAME"
				+ "				   ELSE RL2.BENE_RELATION"
				+ "			  END AS BENE_RELATION,"
				+ "			  RL2.BENE_NAME,"
				+ "			  RL2.BENE_BIRTHDAY,"
				+ "			  RL2.SUCCESS,"
				+ "			  RL2.LOCUS,"
				+ "			  RL2.LAST_STEP,"
				+ "			  RL2.START_TIME,"
				+ "			  RL2.END_TIME,"
				+ "			  RL2.FROM_ID,"
				+ "			  RL2.CLUE_TYPE,"
				+ "			  RL2.IS_CLUE,"
				+ "			  RL2.PROCESS_COUNT,"
				+ "			  RL2.USER_NAME,"
				+ "			  RL2.IS_BUY,"
				+ "			  RL2.MOBILE"
				+ "   FROM    Tmp_rsRl2 RL2"
				+ "	LEFT JOIN Tmp_rela DR"
				+ "	    ON    TRIM(COALESCE(RL2.BENE_RELATION,'')) = TRIM(DR.BENE_RELA)";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_rsRl3");
	}

	/**
	 * 
	 * @Description: 将被保险人与投保人关系转为中文字符
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月29日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame resolveRelation2(HiveContext sqlContext) {
		String sql = "SELECT  /*+MAPJOIN(DR)*/"
				+ "			  RL1.APP_TYPE,"
				+ "			  RL1.APP_ID,"
				+ "			  RL1.TERMINAL_ID,"
				+ "			  RL1.USER_TYPE,"
				+ "			  RL1.USER_ID,"
				+ "			  RL1.STATUS,"
				+ "			  RL1.INTERRUPT_REASON,"
				+ "			  RL1.FORM_ID,"
				+ "			  RL1.LRT_ID,"
				+ "			  RL1.LRT_NAME,"
				+ "			  RL1.SALE_MODE,"
				+ "			  RL1.BABY_GENDER,"
				+ "			  RL1.BABY_BIRTHDAY,"
				+ "			  RL1.INSURE_TYPE,"
				+ "			  RL1.SEX,"
				+ "			  RL1.BABY_RELATION,"
				+ "			  RL1.P_BIRTHDAY,"
				+ "			  RL1.PAY_TYPE,"
				+ "			  RL1.PAY_PERIOD,"
				+ "			  RL1.PH_NAME,"
				+ "			  RL1.PH_BIRTHDAY,"
				+ "			  RL1.PH_EMAIL,"
				+ "			  RL1.PH_INDUSTRY,"
				+ "			  RL1.PH_WORKTYPE,"
				+ "			  CASE WHEN DR.RE_NAME <> '' AND DR.RE_NAME IS NOT NULL THEN DR.RE_NAME"
				+ "				   ELSE RL1.INS_RELATION"
				+ "			  END AS INS_RELATION,"
				+ "			  RL1.INS_NAME,"
				+ "			  RL1.INS_BIRTHDAY,"
				+ "			  RL1.BENE_RELATION,"
				+ "			  RL1.BENE_NAME,"
				+ "			  RL1.BENE_BIRTHDAY,"
				+ "			  RL1.SUCCESS,"
				+ "			  RL1.LOCUS,"
				+ "			  RL1.LAST_STEP,"
				+ "			  RL1.START_TIME,"
				+ "			  RL1.END_TIME,"
				+ "			  RL1.FROM_ID,"
				+ "			  RL1.CLUE_TYPE,"
				+ "			  RL1.IS_CLUE,"
				+ "			  RL1.PROCESS_COUNT,"
				+ "			  RL1.USER_NAME,"
				+ "			  RL1.IS_BUY,"
				+ "			  RL1.MOBILE"
				+ "   FROM    Tmp_rsRl1 RL1"
				+ "	LEFT JOIN Tmp_rela DR"
				+ "	    ON    TRIM(COALESCE(RL1.INS_RELATION,'')) = TRIM(DR.BENE_RELA)";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_rsRl2");
	}

	
	/**
	 * 
	 * @Description: 将宝贝与投保人关系转为中文字符
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月29日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame resolveRelation1(HiveContext sqlContext) {
		String sql = "SELECT  /*+MAPJOIN(DR)*/"
				+ "			  TBY.APP_TYPE,"
				+ "			  TBY.APP_ID,"
				+ "			  TBY.TERMINAL_ID,"
				+ "			  TBY.USER_TYPE,"
				+ "			  TBY.USER_ID,"
				+ "			  TBY.STATUS,"
				+ "			  TBY.INTERRUPT_REASON,"
				+ "			  TBY.FORM_ID,"
				+ "			  TBY.LRT_ID,"
				+ "			  TBY.LRT_NAME,"
				+ "			  CASE WHEN LOWER(TBY.SALE_MODE) = 'presell' THEN '预售'"
				+ "			       WHEN LOWER(TBY.SALE_MODE) = 'online' THEN '销售'"
				+ "				   ELSE TBY.SALE_MODE"
				+ "			  END AS SALE_MODE,"
				+ "			  CASE WHEN TBY.BABY_GENDER = 'F' THEN '女'"
				+ "				   WHEN TBY.BABY_GENDER = 'M' THEN '男'"
				+ "				   ELSE TBY.BABY_GENDER"
				+ "			  END AS BABY_GENDER,"
				+ "			  TBY.BABY_BIRTHDAY,"
				+ "			  CASE WHEN TBY.INSURE_TYPE = 'M' THEN '自己'"
				+ "				   WHEN TBY.INSURE_TYPE = 'C' THEN '未成年人'"
				+ "				   WHEN TBY.INSURE_TYPE = 'P' THEN '父母'"
				+ "				   WHEN TBY.INSURE_TYPE = 'S' THEN '配偶'"
				+ "				   ELSE TBY.INSURE_TYPE"
				+ "			  END AS INSURE_TYPE,"
				+ "			  CASE WHEN TBY.SEX = 'F' THEN '女'"
				+ "				   WHEN TBY.SEX = 'M' THEN '男'"
				+ "				   ELSE TBY.SEX"
				+ "			  END AS SEX,"
				+ "			  CASE WHEN DR.RE_NAME <> '' AND DR.RE_NAME IS NOT NULL THEN DR.RE_NAME"
				+ "				   ELSE TBY.BABY_RELATION"
				+ "			  END AS BABY_RELATION,"
				+ "			  TBY.P_BIRTHDAY,"
				+ "			  CASE WHEN TBY.PAY_TYPE = 'SP' THEN '一次性缴费'"
				+ "				   WHEN TBY.PAY_TYPE = 'Y5' THEN '五年交'"
				+ "				   WHEN TBY.PAY_TYPE = 'Y10' THEN '十年交'"
				+ "				   WHEN TBY.PAY_TYPE = 'Y15' THEN '十五年交'"
				+ "				   WHEN TBY.PAY_TYPE = 'Y20' THEN '二十年交'"
				+ "			       WHEN TBY.PAY_TYPE = 'Y3' THEN '三年交'"
				+ "				   WHEN TBY.PAY_TYPE = 'Y' THEN '年交'"
				+ "				   WHEN TBY.PAY_TYPE = 'M' THEN '月交'"
				+ "				   ELSE TBY.PAY_TYPE"
				+ "			  END AS PAY_TYPE,"
				+ "			  CASE WHEN TBY.PAY_PERIOD = 'SP' THEN '一次性缴费'"
				+ "				   WHEN TBY.PAY_PERIOD = 'Y5' THEN '五年交'"
				+ "				   WHEN TBY.PAY_PERIOD = 'Y10' THEN '十年交'"
				+ "				   WHEN TBY.PAY_PERIOD = 'Y15' THEN '十五年交'"
				+ "				   WHEN TBY.PAY_PERIOD = 'Y20' THEN '二十年交'"
				+ "			       WHEN TBY.PAY_PERIOD = 'Y3' THEN '三年交'"
				+ "				   WHEN TBY.PAY_PERIOD = 'Y' THEN '年交'"
				+ "				   WHEN TBY.PAY_PERIOD = 'M' THEN '月交'"
				+ "				   ELSE TBY.PAY_PERIOD"
				+ "			  END AS PAY_PERIOD,"
				+ "			  TBY.PH_NAME,"
				+ "			  TBY.PH_BIRTHDAY,"
				+ "			  TBY.PH_EMAIL,"
				+ "			  TBY.PH_INDUSTRY,"
				+ "			  TBY.PH_WORKTYPE,"
				+ "			  TBY.INS_RELATION,"
				+ "			  TBY.INS_NAME,"
				+ "			  TBY.INS_BIRTHDAY,"
				+ "			  TBY.BENE_RELATION,"
				+ "			  TBY.BENE_NAME,"
				+ "			  TBY.BENE_BIRTHDAY,"
				+ "			  CASE WHEN LOWER(TBY.SUCCESS) = 'false' then '支付失败'"
				+ "				   WHEN LOWER(TBY.SUCCESS) = 'true' then '支付成功'"
				+ "				   ELSE TBY.SUCCESS"
				+ "			  END AS SUCCESS,"
				+ "			  TBY.LOCUS,"
				+ "			  TBY.LAST_STEP,"
				+ "			  TBY.START_TIME,"
				+ "			  TBY.END_TIME,"
				+ "			  TBY.FROM_ID,"
				+ "			  TBY.CLUE_TYPE,"
				+ "			  TBY.IS_CLUE,"
				+ "			  TBY.PROCESS_COUNT,"
				+ "			  TBY.USER_NAME,"
				+ "			  TBY.IS_BUY,"
				+ "			  TBY.MOBILE"
				+ "   FROM    TMP_ISBUY TBY"
				+ "	LEFT JOIN Tmp_rela DR"
				+ "	    ON    TRIM(COALESCE(TBY.BABY_RELATION,'')) = TRIM(DR.BENE_RELA)";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_rsRl1");
	}

	/**
	 * 
	 * @Description: 计算所投产品之前是否已经购买
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月29日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getIsBuy(HiveContext sqlContext) {
		//0:已购买;1:未购买
		String sql = "SELECT  CSTM.*,"
				+ "			  CASE WHEN PL.LIA_POLICYNO IS NOT NULL THEN '0'"
				+ "				   ELSE '1'"
				+ "			  END AS IS_BUY"
				+ "	  FROM    TMP_CSTM CSTM"
				+ "	LEFT JOIN P_LIFEINSURE PL"
				+ "	   ON	  CSTM.CUSTOMER_ID = PL.POLICYHOLDER_ID"
				+ "		  AND CSTM.LRT_ID = PL.LRT_ID"
				+ "		  AND CAST(CSTM.START_TIME AS BIGINT) >= CAST(UNIX_TIMESTAMP(PL.LIA_ACCEPTTIME,'yyyy-MM-dd HH:mm:ss') AS BIGINT)";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_IsBuy");
	}

	/**
	 * 
	 * @Description: 获取custom_id及name
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月29日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getCustomerinfo(HiveContext sqlContext) {
		String sql = "SELECT  TAF.*,"
				+ "			  FUF.NAME AS USER_NAME,"
				+ "			  FUF.CUSTOMER_ID"
				+ "	   FROM   TMP_ALLINFO TAF"
				+ "	LEFT JOIN (SELECT DISTINCT "
				+ "					  NAME,"
				+ "					  CUSTOMER_ID,"
				+ "					  MEMBER_ID"
				+ "				 FROM FACT_USERINFO"
				+ "			   WHERE  MEMBER_ID <> ''"
				+ "				 AND  MEMBER_ID IS NOT NULL"
				+ "				 AND  CUSTOMER_ID <> ''"
				+ "				 AND  CUSTOMER_ID IS NOT NULL) FUF"
				+ "	   ON     TAF.USER_TYPE = 'MEM'"
				+ "		 AND  TAF.USER_ID = FUF.MEMBER_ID";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_cstm");
	}

	/**
	 * 
	 * @Description: 获取每条线索当天投保次数
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月29日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getAllClueInfo(HiveContext sqlContext) {
		String sql = "SELECT  UTF.*"
				+ "	   FROM   TMP_USERNTINFO UTF"
				+ "	UNION ALL "
				+ "	  SELECT  UNF.*"
				+ "	   FROM   TMP_USERNULLINFO UNF"
				+ "	UNION ALL "
				+ "	  SELECT  SCF.*"
				+ "	   FROM   TMP_SCCESSINFO SCF";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_AllInfo");
	}

	/**
	 * 
	 * @Description: 获取投保成功当天投保次数
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月29日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getSuccessProcessInfo(HiveContext sqlContext) {
		String sql = "SELECT   TAC.*,"
				+ "			   (COALESCE(CT.PROCESS_COUNT,0) + 1) AS PROCESS_COUNT"
				+ "	   FROM    (SELECT  * "
				+ "				FROM   TMP_ALLCLUE"
				+ "			   WHERE   STATUS = '1') TAC"
				+ " LEFT JOIN (SELECT   TNP.UT_ID,"
				+ "						TNP.LRT_ID,"
				+ "						TNP.DAY,"
				+ "						COUNT(1) AS PROCESS_COUNT"
				+ "				 FROM   (SELECT UT_ID,"
				+ "								LRT_ID,"
				+ "								NEAR_TIME,"
				+ "								END_TIME,"
				+ "								FROM_UNIXTIME(CAST(END_TIME/1000 AS BIGINT),'yyyy-MM-dd') AS DAY"
				+ "						  FROM  TMP_NEARPRO) TNP"
				+ "				LEFT JOIN (SELECT  *"
				+ "						  FROM   TMP_FAILED"
				+ "			   			 WHERE   STATUS = '0' ) TFD"
				+ "					ON     TNP.LRT_ID = TFD.LRT_ID"
				+ "					  AND  TNP.UT_ID = (CASE WHEN TFD.USER_ID <> '' AND TFD.USER_ID IS NOT NULL THEN TFD.USER_ID ELSE TFD.TERMINAL_ID END)"
				+ "					  AND  FROM_UNIXTIME(CAST(TFD.END_TIME/1000 AS BIGINT),'yyyy-MM-dd') = "
				+ "						   FROM_UNIXTIME(CAST(TNP.END_TIME/1000 AS BIGINT),'yyyy-MM-dd')"
				+ "		 			  AND  CAST(TFD.END_TIME AS BIGINT) <= CAST(TNP.END_TIME AS BIGINT)"
				+ "					  AND  CAST(TFD.END_TIME AS BIGINT) > CAST(TNP.NEAR_TIME AS BIGINT)"
				+ "				 GROUP BY  TNP.UT_ID,"
				+ "						   TNP.LRT_ID,"
				+ "						   TNP.DAY"
				+ " 			) CT"
				+ "		ON	 (CASE WHEN TAC.USER_ID IS NOT NULL AND TAC.USER_ID <> '' THEN TAC.USER_ID"
				+ "				   ELSE TAC.TERMINAL_ID"
				+ "			  END) = CT.UT_ID"
				+ "		 AND TAC.LRT_ID = CT.LRT_ID"
				+ "		 AND FROM_UNIXTIME(CAST(TAC.END_TIME/1000 AS BIGINT),'yyyy-MM-dd') = CT.DAY";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_sccessInfo");
	}

	/**
	 * 
	 * @Description: 获取投保成功之前成功投保时间
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月29日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getNearSuccessProcess(HiveContext sqlContext) {
		String sql = "SELECT  CASE WHEN TAC.USER_ID IS NOT NULL AND TAC.USER_ID <> '' THEN TAC.USER_ID"
				+ "				   ELSE TAC.TERMINAL_ID"
				+ "			  END AS UT_ID,"
				+ "			  TAC.LRT_ID,"
				+ "			  TAC.END_TIME,"
				+ "			  MIN(CAST(COALESCE(BF.END_TIME,'0') AS BIGINT)) AS NEAR_TIME"
				+ "	   FROM   (SELECT  * "
				+ "				FROM   TMP_ALLCLUE"
				+ "			   WHERE   STATUS = '1') TAC"
				+ " LEFT JOIN (SELECT  * "
				+ "				FROM   TMP_ALLCLUE"
				+ "			   WHERE   STATUS = '1') BF"
				+ "	   ON     (CASE WHEN TAC.USER_ID <> '' AND TAC.USER_ID IS NOT NULL THEN TAC.USER_ID"
				+ "					ELSE TAC.TERMINAL_ID END) = (CASE WHEN TAC.USER_ID <> '' AND TAC.USER_ID IS NOT NULL THEN BF.USER_ID "
				+ "			  ELSE BF.TERMINAL_ID END)"
				+ "		AND   TAC.LRT_ID = BF.LRT_ID"
				+ "		AND   CAST(TAC.END_TIME AS BIGINT) > CAST(BF.END_TIME AS BIGINT)"
				+ "		AND   FROM_UNIXTIME(CAST(TAC.END_TIME/1000 AS BIGINT),'yyyy-MM-dd') = "
				+ "			  FROM_UNIXTIME(CAST(BF.END_TIME/1000 AS BIGINT),'yyyy-MM-dd')"
				+ "	GROUP BY  (CASE WHEN TAC.USER_ID IS NOT NULL AND TAC.USER_ID <> '' THEN TAC.USER_ID"
				+ "				   ELSE TAC.TERMINAL_ID"
				+ "			  END),"
				+ "			  TAC.LRT_ID,"
				+ "			  TAC.END_TIME";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_nearpro");
	}

	/**
	 * 
	 * @Description: 计算user_id为空的人当天投保次数
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月28日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getUserNullInfo(HiveContext sqlContext) {
		String sql = "SELECT   UNR.*,"
				+ "			   CT.PROCESS_COUNT"
				+ "	   FROM    Tmp_usrnullrecord UNR"
				+ " LEFT JOIN (SELECT   TAC.TERMINAL_ID,"
				+ "						TAC.LRT_ID,"
				/*+ "						TAC.LAST_STEP,"*/
				+ "						UNL.DAY,"
				+ "						COUNT(1) AS PROCESS_COUNT"
				+ "				 FROM   (SELECT  *"
				+ "						  FROM   TMP_ALLCLUE"
				+ "			   			 WHERE   (USER_ID = ''"
				+ "				  		    OR  USER_ID IS  NULL)"
				+ "				  			AND  STATUS = '0' ) TAC"
				+ "				INNER JOIN TMP_USRNULLLCS UNL"
				+ "					ON     TAC.LRT_ID = UNL.LRT_ID"
				+ "					  AND  TAC.TERMINAL_ID = UNL.TERMINAL_ID"
				+ "					  AND  FROM_UNIXTIME(CAST(TAC.END_TIME/1000 AS BIGINT),'yyyy-MM-dd') = UNL.DAY"
				//+ "		 			  AND  CAST(TAC.END_TIME AS BIGINT) <= CAST(UNL.MAX_TIME AS BIGINT)"
				+ "				 GROUP BY  TAC.TERMINAL_ID,"
				+ "						   TAC.LRT_ID,"
				/*+ "						   TAC.LAST_STEP,"*/
				+ "						   UNL.DAY"
				+ " 			) CT"
				+ "		ON	 UNR.TERMINAL_ID = CT.TERMINAL_ID"
				+ "		 AND UNR.LRT_ID = CT.LRT_ID"
				+ "		 AND FROM_UNIXTIME(CAST(UNR.END_TIME/1000 AS BIGINT),'yyyy-MM-dd') = CT.DAY";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_usernullInfo");
	}
	
	/**
	 * 
	 * @Description: 获取投保失败中user_id不为空并且轨迹最长的记录
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月28日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getUserNullMaxRecord(HiveContext sqlContext) {
		String sql = "SELECT  TAC.*"
				+ "	   FROM   (SELECT  *"
				+ "				FROM   TMP_ALLCLUE"
				+ "			   WHERE   (USER_ID = ''"
				+ "				  OR  USER_ID IS  NULL)"
				+ "				  AND  STATUS = '0') TAC"
				+ "	INNER JOIN Tmp_usrnulllcs UNL"
				+ "		ON    TAC.TERMINAL_ID = UNL.TERMINAL_ID"
				+ "		 AND  TAC.LRT_ID = UNL.LRT_ID"
				+ "		 AND  TAC.END_TIME = UNL.MAX_TIME"
				+ "		 AND  LENGTH(TAC.LOCUS) = UNL.LOCUS_LEN";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_usrnullrecord");
	}
	
	/**
	 * 
	 * @Description: 获取投保失败中user_id为空的最长轨迹
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月28日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getUserNullMaxLocus(HiveContext sqlContext) {
		String sql = "SELECT  TAC.DAY,"
				+ "			  TAC.TERMINAL_ID,"
				+ "			  TAC.LRT_ID,"
				+ "			  MAX(TAC.LOCUS_LEN) AS LOCUS_LEN,"
				+ "			  MAX(CAST(TAC.END_TIME AS BIGINT)) AS MAX_TIME"
				+ "	   FROM   (SELECT  FROM_UNIXTIME(CAST(END_TIME/1000 AS BIGINT),'yyyy-MM-dd') AS DAY,"
				+ "			  		   TERMINAL_ID,"
				+ "			  		   LRT_ID,"
				+ "			  		   LENGTH(LOCUS) AS LOCUS_LEN,"
				+ "			 		   END_TIME"
				+ "	   			FROM   TMP_ALLCLUE"
				+ "			   WHERE   (USER_ID = ''"
				+ "				   OR  USER_ID IS  NULL)"
				+ "				  AND  STATUS = '0') TAC"
				+ "	GROUP BY  TAC.TERMINAL_ID," 
				+ "			  TAC.LRT_ID,"
				+ "			  TAC.DAY";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_usrnulllcs");
	}

	/**
	 * 
	 * @Description: 计算user_id不为空的人当天投保次数
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月28日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getUserNotNullInfo(HiveContext sqlContext) {
		String sql = "SELECT   UTR.*,"
				+ "			   CT.PROCESS_COUNT"
				+ "	   FROM    TMP_USRNTRECORD UTR"
				+ " LEFT JOIN (SELECT   TAC.USER_ID,"
				+ "						TAC.LRT_ID,"
				/*+ "						TAC.LAST_STEP,"*/
				+ "						UTL.DAY,"
				+ "						COUNT(1) AS PROCESS_COUNT"
				+ "				 FROM   (SELECT  *"
				+ "						  FROM   TMP_ALLCLUE"
				+ "			   			 WHERE   USER_ID <> ''"
				+ "				  		    AND  USER_ID IS NOT NULL"
				+ "				  			AND  STATUS = '0' ) TAC"
				+ "				INNER JOIN TMP_USRNTLCS UTL"
				+ "					ON     TAC.LRT_ID = UTL.LRT_ID"
				+ "					  AND  TAC.USER_ID = UTL.USER_ID"
				+ "					  AND  FROM_UNIXTIME(CAST(TAC.END_TIME/1000 AS BIGINT),'yyyy-MM-dd') = UTL.DAY"
				//+ "		 			  AND  CAST(TAC.END_TIME AS BIGINT) <= CAST(UTL.MAX_TIME AS BIGINT)"
				+ "				 GROUP BY  TAC.USER_ID,"
				+ "						   TAC.LRT_ID,"
				/*+ "						   TAC.LAST_STEP,"*/
				+ "						   UTL.DAY"
				+ " 			) CT"
				+ "		ON	 UTR.USER_ID = CT.USER_ID"
				+ "		 AND UTR.LRT_ID = CT.LRT_ID"
				+ "		 AND FROM_UNIXTIME(CAST(UTR.END_TIME/1000 AS BIGINT),'yyyy-MM-dd') = CT.DAY";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_userntInfo");
	}

	/**
	 * 
	 * @Description: 获取投保失败中user_id不为空并且轨迹最长的记录
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月28日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getUserNotNullMaxRecord(HiveContext sqlContext) {
		String sql = "SELECT  TAC.*"
				+ "	   FROM   (SELECT  *"
				+ "				FROM   TMP_ALLCLUE"
				+ "			   WHERE   USER_ID <> ''"
				+ "				  AND  USER_ID IS NOT NULL"
				+ "				  AND  STATUS = '0') TAC"
				+ "	INNER JOIN TMP_USRNTLCS UTL"
				+ "		ON    TAC.USER_ID = UTL.USER_ID"
				+ "		 AND  TAC.LRT_ID = UTL.LRT_ID"
				+ "		 AND  TAC.END_TIME = UTL.MAX_TIME"
				+ "		 AND  LENGTH(TAC.LOCUS) = UTL.LOCUS_LEN";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_usrntrecord");
	}

	/**
	 * 
	 * @Description: 获取投保失败中user_id不为空的最长轨迹
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月28日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getUserNotNullMaxLocus(HiveContext sqlContext) {
		String sql = "SELECT  TAC.DAY,"
				+ "			  TAC.USER_ID,"
				+ "			  TAC.LRT_ID,"
				+ "			  MAX(TAC.LOCUS_LEN) AS LOCUS_LEN,"
				+ "			  MAX(CAST(TAC.END_TIME AS BIGINT)) AS MAX_TIME"
				+ "	  FROM    (SELECT  FROM_UNIXTIME(CAST(END_TIME/1000 AS BIGINT),'yyyy-MM-dd') AS DAY,"
				+ "			  		   USER_ID,"
				+ "			  		   LRT_ID,"
				+ "			  		   LENGTH(LOCUS) AS LOCUS_LEN,"
				+ "			  		   END_TIME"
				+ "	   			FROM   TMP_ALLCLUE"
				+ "			   WHERE   USER_ID <> ''"
				+ "				  AND  USER_ID IS NOT NULL"
				+ "				  AND  STATUS = '0') TAC"
				+ "	GROUP BY  TAC.USER_ID," 
				+ "			  TAC.LRT_ID,"
				+ "			  TAC.DAY";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_usrntlcs",DataFrameUtil.CACHETABLE_PARQUET);
	}

	/**
	 * 
	 * @Description: 获取所有线索
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月28日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getAllclue(HiveContext sqlContext) {
		String sql = "SELECT  SCS.*"
				+ "	   FROM   TMP_SUCCESS SCS"
				+ "	  WHERE   SCS.IS_CLUE = '1'"
				+ "	UNION ALL"
				+ "	  SELECT  FLD.*"
				+ "	   FROM   TMP_FAILED FLD"
				+ "	  WHERE   FLD.IS_CLUE = '1'";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_allClue",DataFrameUtil.CACHETABLE_PARQUET);
	}

	/**
	 * 
	 * @Description: 获取所有投保失败的记录并区分是否是线索
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月28日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getAllProcessfailed(HiveContext sqlContext,Map<String,String> config) {
		String failedType = config.get("failedType");
		if(StringUtils.isNotBlank(failedType)){
			failedType = "(" + failedType + ")";
		}else{
			failedType = "('9999')";
		}
		String failedLrtId = config.get("failedLrtId");
		if(StringUtils.isNotBlank(failedLrtId)){
			failedLrtId = "(" + failedLrtId + ")";
		}else{
			failedLrtId = "('0000')";
		}
		String sql = "SELECT  IPE.*,"
				+ "			  CASE WHEN SCS.FORM_ID IS NULL "
				+ "						AND (IPE.CLUE_TYPE IN " + failedType 
				+ "					        OR IPE.LRT_ID IN " + failedLrtId + ") THEN '1'"
				+ "				   ELSE '0'"
				+ "			  END AS IS_CLUE"
				+ "	   FROM	  (SELECT  *"
				+ "				FROM   Tmp_Src"
				+ "			   WHERE   STATUS = '0') IPE"
				+ "	LEFT JOIN TMP_SUCCESS SCS"
				+ "	   ON     (CASE WHEN IPE.USER_ID = '' OR IPE.USER_ID IS NULL THEN spequals(IPE.TERMINAL_ID,SCS.TERMINAL_ID,',')"
				+ "				   ELSE IPE.USER_ID "
				+ "			  END) = (CASE WHEN IPE.USER_ID = '' OR IPE.USER_ID IS NULL THEN '1'"
				+ "				   ELSE SCS.USER_ID "
				+ "			  END)"
				+ "		  AND IPE.LRT_ID = SCS.LRT_ID"
				+ "		  AND CAST(IPE.END_TIME AS BIGINT) <= CAST(COALESCE(SCS.END_TIME,'0') AS BIGINT)"
				+ "		  AND FROM_UNIXTIME(CAST(IPE.END_TIME/1000 AS BIGINT),'yyyy-MM-dd')="
				+ "			  FROM_UNIXTIME(CAST(COALESCE(SCS.END_TIME,'0')/1000 AS BIGINT),'yyyy-MM-dd')"
				+ "		  AND FROM_UNIXTIME(CAST(IPE.START_TIME/1000 AS BIGINT),'yyyy-MM-dd')="
				+ "			  FROM_UNIXTIME(CAST(COALESCE(SCS.START_TIME,'0')/1000 AS BIGINT),'yyyy-MM-dd')";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_failed",DataFrameUtil.CACHETABLE_PARQUET);
	}

	/**
	 * 
	 * @Description: 获取所有投保成功记录并区分是否是线索
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月28日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getAllProcessSccess(HiveContext sqlContext,Map<String,String> config) {
		String successType = config.get("successType");
		if(StringUtils.isNotBlank(successType)){
			successType = "(" + successType + ")";
		}else{
			successType = "('9999')";
		}
		String successLrtId = config.get("successLrtId");
		if(StringUtils.isNotBlank(successLrtId)){
			successLrtId = "(" + successLrtId + ")";
		}else{
			successLrtId = "('0000')";
		}
		String sql = "SELECT  IPE.*,"
				+ "			  CASE WHEN IPE.CLUE_TYPE IN "+ successType 
				+ "				        OR IPE.LRT_ID IN " + successLrtId + " THEN '1'"
				+ "				   ELSE '0'"
				+ "			  END AS IS_CLUE"
				+ "	   FROM	  Tmp_Src IPE"
				+ "	   WHERE  IPE.STATUS = '1'";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_success",DataFrameUtil.CACHETABLE_PARQUET);
	}
	
	/**
	 * 
	 * @Description: 数据保存
	 * @param path
	 * @param isall
	 * @author moyunqing
	 * @date 2016年9月24日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	public void save(DataFrame df,String path, boolean isall) {
		if(!isall){
        	df.save(path, "parquet", SaveMode.Append);
    	}else{
    		if (TK_DataFormatConvertUtil.isExistsPath(path)) {
    			long timeTag = System.currentTimeMillis();
    			String tmpPath = path + timeTag;
    			df.saveAsParquetFile(tmpPath);
    			TK_DataFormatConvertUtil.deletePath(path);
    			TK_DataFormatConvertUtil.renamePath(tmpPath, path);
    		} else {
    			df.saveAsParquetFile(path);
    		}
    	}
	}
	
	/**
     * 
     * @Description: 获取日期字符串
     * @param day
     * @return
     * @author moyunqing
     * @date 2016年9月20日
     * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    private static String getDate(int day) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, day);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		return sdf.format(cal.getTime());
	}
}
