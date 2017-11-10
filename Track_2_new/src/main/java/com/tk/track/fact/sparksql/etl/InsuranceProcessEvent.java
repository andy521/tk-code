package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import com.tk.track.fact.sparksql.desttable.InsuranceProcessTmp;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.fact.sparksql.util.StringParse;
import com.tk.track.util.TK_DataFormatConvertUtil;

/**
 * 
* <p>Title: 泰康大数据部</p>
* <p>Description: 通用投保流程中间表</p>
* <p>Copyright: Copyright (c) 2016</p>
* <p>Company: 大连华信有限公司</p>
* <p>Department: 总公司\数据信息中心\IT外包公司\大连华信总公司\数据信息中心\IT外包公司\大连华信部</p>
* @author moyunqing
* @date   2016年9月22日
* @version 1.0
 */
public class InsuranceProcessEvent implements Serializable {
	
	private static final long serialVersionUID = -460929149733429642L;
	
	private static final String sca_nw = TK_DataFormatConvertUtil.getNetWorkSchema();

	/**
	 * 
	 * @Description: 生成投保流程中间表
	 * @param sqlContext
	 * @param config
	 * @param isall
	 * @return
	 * @author moyunqing
	 * @date 2016年10月12日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	public DataFrame getInsuranceProcess(HiveContext sqlContext,Map<String, Map<String, String>> config,boolean isall){
//		getSrcData(sqlContext,isall);
//		getFloorUserId(sqlContext);
//		getSrcFloor(sqlContext);
//		addFloorUserId2Src(sqlContext);
//		getLastLogin(sqlContext);
//		addUserIdCeil2Src(sqlContext);
//		fillUserId(sqlContext);
		//解析label和customize信息
		//getFullSrcData(sqlContext);
		sqlContext.createDataFrame(analysisFields(getSrcData(sqlContext,isall)),InsuranceProcessTmp.class).registerTempTable("Tmp_analsrc");
		updateFormId(sqlContext);
		getFormMap(sqlContext);
		getNearstFormID(sqlContext);
		addFormId2Src(sqlContext);
		addUserIdByFormId(sqlContext);
		getPremiumTestPage(sqlContext);
		getPremiumTestBtn(sqlContext);
		getPersonInfoPage(sqlContext);
		getPersonInfoBtn(sqlContext);
		getHealthInformPage(sqlContext);
		getHealthInformBtn(sqlContext);
		getPaymentPage(sqlContext);
		getPaymentBtn(sqlContext);
		getPaymentBackPage(sqlContext);
		combineByFormIdNotNull(sqlContext);
		combineTerminalIdByFormId(sqlContext);
		combineByFormIdNull(sqlContext);
		combineTerminalIdByUserId(sqlContext);
		getCombineRecord(sqlContext);
		getDliferisktype(sqlContext);
		addLrtName2Record(sqlContext);
		getLocusAndStatus(sqlContext);
		return getClueType(sqlContext,config);
	}

	private DataFrame getDliferisktype(HiveContext sqlContext) {
		String sql = "SELECT DISTINCT"
				+ "			 LRT_ID,"
				+ "			 LRT_NAME"
				+ "	   FROM  " + sca_nw + "d_liferisktype";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_lrt",DataFrameUtil.CACHETABLE_EAGER);
	}

	/**
	 * 
	 * @Description: 获取所有的原始数据
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年10月12日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getFullSrcData(HiveContext sqlContext) {
		String sql = "SELECT TSC.APP_TYPE,"
				+ "			 TSC.APP_ID,"
				+ "			 TSC.EVENT,"
				+ "			 TSC.SUBTYPE,"
				+ "			 TSC.TERMINAL_ID,"
				+ "			 TSC.USER_ID,"
				+ "			 TSC.LABEL,"
				+ "			 TSC.CUSTOM_VAL,"
				+ "			 TSC.VISIT_TIME,"
				+ "			 TSC.FROM_ID"
				+ "	   FROM  TMP_SRC TSC"
				+ "	LEFT JOIN TMP_FLUID FID"
				+ "		ON   TSC.TERMINAL_ID = FID.TERMINAL_ID"
				+ "	 WHERE FID.TERMINAL_ID IS NULL"
				+ " UNION ALL"
				+ "	 SELECT  TFD.APP_TYPE,"
				+ "			 TFD.APP_ID,"
				+ "			 TFD.EVENT,"
				+ "			 TFD.SUBTYPE,"
				+ "			 TFD.TERMINAL_ID,"
				+ "			 TFD.USER_ID,"
				+ "			 TFD.LABEL,"
				+ "			 TFD.CUSTOM_VAL,"
				+ "			 TFD.VISIT_TIME,"
				+ "			 TFD.FROM_ID "
				+ "	  FROM   TMP_FLUID TFD";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_fulldata");
	}

	/**
	 * 
	 * @Description: 通过formId补全user_id
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年10月12日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame addUserIdByFormId(HiveContext sqlContext) {
		String sql = "SELECT FDS.APP_TYPE,"
				+ "			 FDS.APP_ID,"
				+ "			 FDS.EVENT,"
				+ "			 FDS.SUBTYPE,"
				+ "			 FDS.TERMINAL_ID,"
				+ "			 CASE WHEN FDS.USER_ID <> '' AND FDS.USER_ID IS NOT NULL THEN FDS.USER_ID"
				+ "				  WHEN FDSNTL.USER_ID <> '' AND FDSNTL.USER_ID IS NOT NULL THEN FDSNTL.USER_ID"
				+ "				  ELSE ''"
				+ "			 END AS USER_ID,"
				+ "			 FDS.VISIT_TIME,"
				+ "			 FDS.FROM_ID,"
				+ "			 FDS.LRT_ID,"
				+ "			 FDS.SALE_MODE,"
				+ "			 FDS.BABY_GENDER,"
				+ "			 FDS.BABY_BIRTHDAY,"
				+ "			 FDS.INSURE_TYPE,"
				+ "			 FDS.SEX,"
				+ "			 FDS.BABY_RELATION,"
				+ "			 FDS.P_BIRTHDAY,"
				+ "			 FDS.PAY_TYPE,"
				+ "			 FDS.PAY_PERIOD,"
				+ "			 FDS.PH_NAME,"
				+ "			 FDS.PH_BIRTHDAY,"
				+ "			 FDS.PH_EMAIL,"
				+ "			 FDS.PH_INDUSTRY,"
				+ "			 FDS.PH_WORKTYPE,"
				+ "			 FDS.INS_RELATION,"
				+ "			 FDS.INS_NAME,"
				+ "			 FDS.INS_BIRTHDAY,"
				+ "			 FDS.BENE_RELATION,"
				+ "			 FDS.BENE_NAME,"
				+ "			 FDS.BENE_BIRTHDAY,"
				+ "			 FDS.SUCCESS,"
				+ "			 FDS.FORM_ID"
				+ "	   FROM  (SELECT * "
				+ "			   FROM TMP_FRMID2SRC "
				+ "			  WHERE FORM_ID IS NOT NULL "
				+ "			    AND FORM_ID <> ''"
				+ "			    AND (USER_ID = ''"
				+ "				OR  USER_ID IS NULL)) FDS"
				+ " LEFT JOIN (SELECT FORM_ID,"
				+ "				      USER_ID,"
				+ "					  MAX(VISIT_TIME)"
				+ "			    FROM  TMP_FRMID2SRC"
				+ "			   WHERE  FORM_ID <> ''"
				+ "				  AND FORM_ID IS NOT NULL"
				+ "				  AND USER_ID <> ''"
				+ "				  AND USER_ID IS NOT NULL"
				+ "			   GROUP BY FORM_ID,"
				+ "						USER_ID) FDSNTL"
				+ "	   ON    FDS.FORM_ID = FDSNTL.FORM_ID"
				+ "	UNION ALL"
				+ "	  SELECT APP_TYPE,"
				+ "			 APP_ID,"
				+ "			 EVENT,"
				+ "			 SUBTYPE,"
				+ "			 TERMINAL_ID,"
				+ "			 USER_ID,"
				+ "			 VISIT_TIME,"
				+ "			 FROM_ID,"
				+ "			 LRT_ID,"
				+ "			 SALE_MODE,"
				+ "			 BABY_GENDER,"
				+ "			 BABY_BIRTHDAY,"
				+ "			 INSURE_TYPE,"
				+ "			 SEX,"
				+ "			 BABY_RELATION,"
				+ "			 P_BIRTHDAY,"
				+ "			 PAY_TYPE,"
				+ "			 PAY_PERIOD,"
				+ "			 PH_NAME,"
				+ "			 PH_BIRTHDAY,"
				+ "			 PH_EMAIL,"
				+ "			 PH_INDUSTRY,"
				+ "			 PH_WORKTYPE,"
				+ "			 INS_RELATION,"
				+ "			 INS_NAME,"
				+ "			 INS_BIRTHDAY,"
				+ "			 BENE_RELATION,"
				+ "			 BENE_NAME,"
				+ "			 BENE_BIRTHDAY,"
				+ "			 SUCCESS,"
				+ "			 FORM_ID"
				+ "	 FROM    TMP_FRMID2SRC"
				+ "	 WHERE   FORM_ID = ''"
				+ "		  OR FROM_ID IS NULL"
				+ "		  OR (FORM_ID <> ''"
				+ "		 AND FORM_ID IS NOT NULL"
				+ "		 AND USER_ID <> ''"
				+ "		 AND USER_ID IS NOT NULL)";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_srcData",DataFrameUtil.CACHETABLE_PARQUET);
	}


	/**
	 * 
	 * @Description:向合并后的记录添加lrt_name
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年10月11日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame addLrtName2Record(HiveContext sqlContext) {
		String sql = "SELECT /*+MAPJOIN(LDM)*/"
				+ "			 CR.*,"
				+ "			 LDM.LRT_NAME"
				+ "	   FROM  TMP_COMBINERECORD CR"
				+ "	LEFT JOIN Tmp_lrt LDM"
				+ "	   ON    CR.LRT_ID = LDM.LRT_ID";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_fulllrtname");
	}

	/**
	 * 
	 * @Description: 获取当前浏览时间（user_id为空）之后FACT_TERMINALMAP中最近的记录
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年10月11日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getFloorUserId(HiveContext sqlContext) {
        String hql = "SELECT TMAP.APP_TYPE,"
        		+ "			 TMAP.APP_ID,"
        		+ "			 SUE.TERMINAL_ID,"
                + "          SUE.VISIT_TIME," 
                + "      	 CAST(MIN(CAST(COALESCE(TMAP.CLIENTTIME,'0') AS BIGINT)) AS STRING) AS NEAR_TIME"
                + "    FROM  Tmp_src SUE, FACT_TERMINALMAP TMAP"
                + "   WHERE  TMAP.TERMINAL_ID = SUE.TERMINAL_ID"
                + "		AND  SUE.APP_TYPE = TMAP.APP_TYPE"
                + "     AND  SUE.APP_ID =  TMAP.APP_ID" 
                + "     AND  TMAP.CLIENTTIME >= SUE.VISIT_TIME"
                + "   GROUP  BY TMAP.APP_TYPE,TMAP.APP_ID,SUE.TERMINAL_ID, SUE.VISIT_TIME ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_floor",DataFrameUtil.CACHETABLE_PARQUET);
    }
	
	
	private DataFrame getSrcFloor(HiveContext sqlContext){
		String sql = "SELECT PSRC.APP_TYPE,"
				+ "			 		   PSRC.APP_ID,"
				+ "			 		   PSRC.EVENT,"
				+ "			 		   PSRC.SUBTYPE,"
				+ "			 		   PSRC.TERMINAL_ID,"
				+ "			 		   PSRC.USER_ID,"
				+ "			 		   PSRC.LABEL,"
				+ "			 		   PSRC.CUSTOM_VAL,"
				+ "			 		   PSRC.VISIT_TIME,"
				+ "			 		   PSRC.FROM_ID,"
                + "                    FLR.NEAR_TIME"
                + "              FROM  TMP_SRC PSRC, TMP_FLOOR FLR "
                + "             WHERE  PSRC.TERMINAL_ID = FLR.TERMINAL_ID"
                + "				  AND  PSRC.APP_TYPE = FLR.APP_TYPE"
                + "				  AND  PSRC.APP_ID = FLR.APP_ID "
                + "               AND  PSRC.VISIT_TIME = FLR.VISIT_TIME ";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_sfr");
	}
	
	/**
	 * 
	 * @Description: 补全比FACT_TERMINALMAP中访问时间小的记录的userId
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年10月11日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame addFloorUserId2Src(HiveContext sqlContext) {
		String hql = "SELECT   SFR.APP_TYPE," 
                + "            SFR.APP_ID,"
                + " 		   CASE WHEN  SFR.USER_ID <> '' AND SFR.USER_ID IS NOT NULL THEN SFR.USER_ID"
                + "      		    WHEN  SFR.USER_ID = '' OR SFR.USER_ID IS NULL THEN TMAP.USER_ID"
                + "				    ELSE ''" 
                + " 		   END AS USER_ID,"
                + "            SFR.EVENT,"
                + "            SFR.SUBTYPE,"
                + "            SFR.TERMINAL_ID,"
                + "            SFR.LABEL,"
                + "            SFR.CUSTOM_VAL,"
                + "            SFR.VISIT_TIME," 
                + "            SFR.FROM_ID"
                + "      FROM  FACT_TERMINALMAP TMAP, "
                + "            Tmp_sfr SFR "
                + "     WHERE  SFR.NEAR_TIME = TMAP.CLIENTTIME "
                + "       AND  SFR.TERMINAL_ID = TMAP.TERMINAL_ID"
                + "		  AND  SFR.APP_TYPE = TMAP.APP_TYPE"
                + "		  AND  SFR.APP_ID = TMAP.APP_ID";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_fu2s");
    }
	
	// 筛选出各个终端最后一次登陆的信息(无USER_ID信息)
    private DataFrame getLastLogin(HiveContext sqlContext) {
        String hql = "SELECT  TMAP.APP_TYPE,"
        		+ "		   	  TMAP.APP_ID,"
        		+ "		   	  TMAP.TERMINAL_ID, "
                + "        	  CAST(MAX(CAST(TMAP.CLIENTTIME AS BIGINT)) AS STRING) AS MAXTIME "
                + "     FROM  FACT_TERMINALMAP TMAP " 
                + " GROUP BY  TMAP.APP_TYPE,TMAP.APP_ID,TMAP.TERMINAL_ID";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_lstLg",DataFrameUtil.CACHETABLE_EAGER);
    }
	
    /**
     * 
     * @Description: 补全user_id为空的记录（最后一次登录之后的记录）
     * @param sqlContext
     * @return
     * @author moyunqing
     * @date 2016年10月11日
     * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    private DataFrame addUserIdCeil2Src(HiveContext sqlContext) {
        String hql = "SELECT   NUID.APP_TYPE," 
                + "            NUID.APP_ID,"
                + " 		   CASE WHEN  NUID.USER_ID <> '' AND NUID.USER_ID IS NOT NULL THEN NUID.USER_ID"
                + "      		    WHEN  NUID.USER_ID = '' OR NUID.USER_ID IS NULL THEN FUID.USER_ID"
                + "				    ELSE ''" 
                + " 		   END AS USER_ID,"
                + "            NUID.EVENT,"
                + "            NUID.SUBTYPE,"
                + "            NUID.TERMINAL_ID,"
                + "            NUID.LABEL,"
                + "            NUID.CUSTOM_VAL,"
                + "            NUID.VISIT_TIME," 
                + "            NUID.FROM_ID" 
                + "     FROM  (SELECT   SUE.* "
                + "               FROM  Tmp_src SUE, Tmp_lstLg FUP2"
                + "              WHERE  SUE.TERMINAL_ID = FUP2.TERMINAL_ID"
                + "				   AND  SUE.APP_TYPE = FUP2.APP_TYPE"
                + "				   AND  SUE.APP_ID = FUP2.APP_ID "
                + "                AND  SUE.VISIT_TIME > FUP2.MAXTIME) NUID, "
                + "           (SELECT   TMAP.* "
                + "               FROM  FACT_TERMINALMAP TMAP "
                + "         INNER JOIN  Tmp_lstLg FUP2" 
                + "                 ON  TMAP.TERMINAL_ID = FUP2.TERMINAL_ID "
                + "				   AND  TMAP.APP_TYPE = FUP2.APP_TYPE"
                + "				   AND  TMAP.APP_ID = FUP2.APP_ID"
                + "                AND  TMAP.CLIENTTIME = FUP2.MAXTIME) FUID "
                + "    WHERE  NUID.TERMINAL_ID = FUID.TERMINAL_ID"
                + "	    AND   NUID.APP_TYPE = FUID.APP_TYPE"
                + "	    AND   NUID.APP_ID = FUID.APP_ID";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_uc2s");
    }
    
    /**
     * 
     * @Description: 补全user_id
     * @param sqlContext
     * @return
     * @author moyunqing
     * @date 2016年10月11日
     * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    private DataFrame fillUserId(HiveContext sqlContext) {
        String hql = "SELECT   fu2s.* " 
                + "      FROM  Tmp_fu2s fu2s "
                + " UNION ALL " 
                + "    SELECT  uc2s.* "
                + "      FROM  Tmp_uc2s uc2s ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_fluid");
    }
	
	/**
	 * 
	 * @Description: 更新form_id(排除异常form_id)
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年10月8日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame updateFormId(HiveContext sqlContext) {
		String sql = "SELECT ALSRC.APP_TYPE,"
				+ "			 ALSRC.APP_ID,"
				+ "			 ALSRC.EVENT,"
				+ "			 ALSRC.SUBTYPE,"
				+ "			 ALSRC.TERMINAL_ID,"
				+ "			 ALSRC.USER_ID,"
				+ "			 ALSRC.VISIT_TIME,"
				+ "			 ALSRC.FROM_ID,"
				+ "			 REGEXP_EXTRACT(TRIM(ALSRC.FORM_ID),'(^[0-9]+$)') AS FORM_ID,"
				+ "			 ALSRC.LRT_ID,"
				+ "			 ALSRC.SALE_MODE,"
				+ "			 ALSRC.BABY_GENDER,"
				+ "			 ALSRC.BABY_BIRTHDAY,"
				+ "			 ALSRC.INSURE_TYPE,"
				+ "			 ALSRC.SEX,"
				+ "			 ALSRC.BABY_RELATION,"
				+ "			 ALSRC.P_BIRTHDAY,"
				+ "			 ALSRC.PAY_TYPE,"
				+ "			 ALSRC.PAY_PERIOD,"
				+ "			 ALSRC.PH_NAME,"
				+ "			 ALSRC.PH_BIRTHDAY,"
				+ "			 ALSRC.PH_EMAIL,"
				+ "			 ALSRC.PH_INDUSTRY,"
				+ "			 ALSRC.PH_WORKTYPE,"
				+ "			 ALSRC.INS_RELATION,"
				+ "			 ALSRC.INS_BIRTHDAY,"
				+ "			 ALSRC.INS_NAME,"
				+ "			 ALSRC.BENE_RELATION,"
				+ "			 ALSRC.BENE_NAME,"
				+ "			 ALSRC.BENE_BIRTHDAY,"
				+ "			 ALSRC.SUCCESS"
				+ "	  FROM   TMP_ANALSRC ALSRC"
				+ "	  WHERE  (ALSRC.USER_ID <> '' AND ALSRC.USER_ID IS NOT NULL)"
				+ "		 OR  (ALSRC.FORM_ID <> '' AND ALSRC.FORM_ID IS NOT NULL)";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_userIdAll",DataFrameUtil.CACHETABLE_PARQUET);
	}

	/**
	 * 
	 * @Description: 合并terminalId(formid非空部分)
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月27日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame combineTerminalIdByFormId(HiveContext sqlContext) {
		String sql = "SELECT FNN.APP_TYPE,"
				+ "			 FNN.APP_ID,"
				+ "			 FNN.TERMINAL_ID,"
				+ "			 FNN.USER_TYPE,"
				+ "			 FNN.USER_ID,"
				+ "			 FNN.FORM_ID,"
				+ "			 FNN.LRT_ID,"
				+ "			 FNN.SALE_MODE,"
				+ "			 FNN.BABY_GENDER,"
				+ "			 FNN.BABY_BIRTHDAY,"
				+ "			 FNN.INSURE_TYPE,"
				+ "			 FNN.SEX,"
				+ "			 FNN.BABY_RELATION,"
				+ "			 FNN.P_BIRTHDAY,"
				+ "			 FNN.PAY_TYPE,"
				+ "			 FNN.PAY_PERIOD,"
				+ "			 FNN.PH_NAME,"
				+ "			 FNN.PH_BIRTHDAY,"
				+ "			 FNN.PH_EMAIL,"
				+ "			 FNN.PH_INDUSTRY,"
				+ "			 FNN.PH_WORKTYPE,"
				+ "			 FNN.INS_RELATION,"
				+ "			 FNN.INS_NAME,"
				+ "			 FNN.INS_BIRTHDAY,"
				+ "			 FNN.BENE_RELATION,"
				+ "			 FNN.BENE_NAME,"
				+ "			 FNN.BENE_BIRTHDAY,"
				+ "			 FNN.LOCUS,"
				+ "			 FNN.SUCCESS,"
				+ "			 FNN.LAST_STEP,"
				+ "			 CAST(MIN(CAST(FNN.START_TIME AS BIGINT)) AS STRING) START_TIME,"
				+ "			 CAST(MAX(CAST(FNN.END_TIME AS BIGINT)) AS STRING) END_TIME,"
				+ "			 FNN.FROM_ID "
				+ "  FROM  (SELECT APP_TYPE,"
				+ "			 	   APP_ID,"
				+ "			 	   genuniq(CONCAT_WS(',',"
				+ "						COALESCE(TPG_TERMINAL_ID,''),"
				+ "						COALESCE(TBN_TERMINAL_ID,''),"
				+ "			 			COALESCE(PSPG_TERMINAL_ID,''),"
				+ "			 			COALESCE(PSBTN_TERMINAL_ID,''),"
				+ "			 			COALESCE(HIP_TERMINAL_ID,''),"
				+ "			 			COALESCE(HIB_TERMINAL_ID,''),"
				+ "			 			COALESCE(PMP_TERMINAL_ID,''),"
				+ "			 			COALESCE(PMB_TERMINAL_ID,''),"
				+ "			 			COALESCE(PMBP_TERMINAL_ID,'')"
				+ "			 		),',','^[0-9]+$') AS TERMINAL_ID,"
				+ "			 		USER_TYPE,"
				+ "			 		USER_ID,"
				+ "			 		FORM_ID,"
				+ "			 		LRT_ID,"
				+ "			 		SALE_MODE,"
				+ "			 		BABY_GENDER,"
				+ "			 		BABY_BIRTHDAY,"
				+ "			 		INSURE_TYPE,"
				+ "			 		SEX,"
				+ "			 		BABY_RELATION,"
				+ "			 		P_BIRTHDAY,"
				+ "			 		PAY_TYPE,"
				+ "			 		PAY_PERIOD,"
				+ "			 		PH_NAME,"
				+ "			 		PH_BIRTHDAY,"
				+ "			 		PH_EMAIL,"
				+ "			 		PH_INDUSTRY,"
				+ "			 		PH_WORKTYPE,"
				+ "			 		INS_RELATION,"
				+ "			 		INS_NAME,"
				+ "			 		INS_BIRTHDAY,"
				+ "			 		BENE_RELATION,"
				+ "			 		BENE_NAME,"
				+ "			 		BENE_BIRTHDAY,"
				+ "			 		CONCAT(TPG_EVENT,"
				+ "			 			   TBN_SUBTYPE,"
				+ "						   PSPG_EVENT,"
				+ "						   PSBTN_SUBTYPE,"
				+ "				    	   HIP_EVENT,"
				+ "				    	   HIB_SUBTYPE,"
				+ "						   PMP_EVENT,"
				+ "				    	   PMB_SUBTYPE,"
				+ "						   PMBP_EVENT"
				+ "			 		) AS LOCUS,"
				+ "			  		SUCCESS,"
				+ "			 		LAST_STEP,"
				+ "			 		START_TIME,"
				+ "			 		END_TIME,"
				+ "			 		FROM_ID"
				+ "	  			FROM   Tmp_FrmIdNotNull) FNN"
				+ "	GROUP BY FNN.APP_TYPE,"
				+ "			 FNN.APP_ID,"
				+ "			 FNN.TERMINAL_ID,"
				+ "			 FNN.USER_TYPE,"
				+ "			 FNN.USER_ID,"
				+ "			 FNN.FORM_ID,"
				+ "			 FNN.LRT_ID,"
				+ "			 FNN.SALE_MODE,"
				+ "			 FNN.BABY_GENDER,"
				+ "			 FNN.BABY_BIRTHDAY,"
				+ "			 FNN.INSURE_TYPE,"
				+ "			 FNN.SEX,"
				+ "			 FNN.BABY_RELATION,"
				+ "			 FNN.P_BIRTHDAY,"
				+ "			 FNN.PAY_TYPE,"
				+ "			 FNN.PAY_PERIOD,"
				+ "			 FNN.PH_NAME,"
				+ "			 FNN.PH_BIRTHDAY,"
				+ "			 FNN.PH_EMAIL,"
				+ "			 FNN.PH_INDUSTRY,"
				+ "			 FNN.PH_WORKTYPE,"
				+ "			 FNN.INS_RELATION,"
				+ "			 FNN.INS_NAME,"
				+ "			 FNN.INS_BIRTHDAY,"
				+ "			 FNN.BENE_RELATION,"
				+ "			 FNN.BENE_NAME,"
				+ "			 FNN.BENE_BIRTHDAY,"
				+ "			 FNN.LOCUS,"
				+ "			 FNN.SUCCESS,"
				+ "			 FNN.LAST_STEP,"
				+ "			 FNN.FROM_ID";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_tformId");
		
	}

	/**
	 * 
	 * @Description: 合并terminalId(formid为空部分)
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月27日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame combineTerminalIdByUserId(HiveContext sqlContext) {
		String sql = "SELECT DISTINCT"
				+ "			 APP_TYPE,"
				+ "			 APP_ID,"
				+ "			 TPG_TERMINAL_ID AS TERMINAL_ID,"
				+ "			 USER_TYPE,"
				+ "			 USER_ID,"
				+ "			 FORM_ID,"
				+ "			 LRT_ID,"
				+ "			 SALE_MODE,"
				+ "			 BABY_GENDER,"
				+ "			 BABY_BIRTHDAY,"
				+ "			 INSURE_TYPE,"
				+ "			 SEX,"
				+ "			 BABY_RELATION,"
				+ "			 P_BIRTHDAY,"
				+ "			 PAY_TYPE,"
				+ "			 PAY_PERIOD,"
				+ "			 PH_NAME,"
				+ "			 PH_BIRTHDAY,"
				+ "			 PH_EMAIL,"
				+ "			 PH_INDUSTRY,"
				+ "			 PH_WORKTYPE,"
				+ "			 INS_RELATION,"
				+ "			 INS_NAME,"
				+ "			 INS_BIRTHDAY,"
				+ "			 BENE_RELATION,"
				+ "			 BENE_NAME,"
				+ "			 BENE_BIRTHDAY,"
				+ "			 CONCAT(TPG_EVENT,"
				+ "			 		TBN_SUBTYPE,"
				+ "					PSPG_EVENT,"
				+ "					PSBTN_SUBTYPE,"
				+ "				    HIP_EVENT,"
				+ "				    HIB_SUBTYPE,"
				+ "					PMP_EVENT,"
				+ "				    PMB_SUBTYPE,"
				+ "					PMBP_EVENT"
				+ "			 ) AS LOCUS,"
				+ "			 SUCCESS,"
				+ "			 LAST_STEP,"
				+ "			 START_TIME,"
				+ "			 END_TIME,"
				+ "			 FROM_ID"
				+ "	  FROM   Tmp_FrmIdNull"
				+ "  WHERE   USER_ID = '' "
				+ "		 OR  USER_ID IS NULL"
				+ "	UNION ALL "
				+ "	  SELECT FDN.APP_TYPE,"
				+ "			 FDN.APP_ID,"
				+ "			 FDN.TERMINAL_ID,"
				+ "			 FDN.USER_TYPE,"
				+ "			 FDN.USER_ID,"
				+ "			 FDN.FORM_ID,"
				+ "			 FDN.LRT_ID,"
				+ "			 FDN.SALE_MODE,"
				+ "			 FDN.BABY_GENDER,"
				+ "			 FDN.BABY_BIRTHDAY,"
				+ "			 FDN.INSURE_TYPE,"
				+ "			 FDN.SEX,"
				+ "			 FDN.BABY_RELATION,"
				+ "			 FDN.P_BIRTHDAY,"
				+ "			 FDN.PAY_TYPE,"
				+ "			 FDN.PAY_PERIOD,"
				+ "			 FDN.PH_NAME,"
				+ "			 FDN.PH_BIRTHDAY,"
				+ "			 FDN.PH_EMAIL,"
				+ "			 FDN.PH_INDUSTRY,"
				+ "			 FDN.PH_WORKTYPE,"
				+ "			 FDN.INS_RELATION,"
				+ "			 FDN.INS_NAME,"
				+ "			 FDN.INS_BIRTHDAY,"
				+ "			 FDN.BENE_RELATION,"
				+ "			 FDN.BENE_NAME,"
				+ "			 FDN.BENE_BIRTHDAY,"
				+ "			 FDN.LOCUS,"
				+ "			 FDN.SUCCESS,"
				+ "			 FDN.LAST_STEP,"
				+ "			 CAST(MIN(CAST(FDN.START_TIME AS BIGINT)) AS STRING) START_TIME,"
				+ "			 CAST(MAX(CAST(FDN.END_TIME AS BIGINT)) AS STRING) END_TIME,"
				+ "			 FDN.FROM_ID"
				+ "	 FROM (SELECT APP_TYPE,"
				+ "			 APP_ID,"
				+ "			 genuniq(CONCAT_WS(',',"
				+ "					COALESCE(TPG_TERMINAL_ID,''),"
				+ "					COALESCE(TBN_TERMINAL_ID,''),"
				+ "			 		COALESCE(PSPG_TERMINAL_ID,''),"
				+ "			 		COALESCE(PSBTN_TERMINAL_ID,''),"
				+ "			 		COALESCE(HIP_TERMINAL_ID,''),"
				+ "			 		COALESCE(HIB_TERMINAL_ID,''),"
				+ "			 		COALESCE(PMP_TERMINAL_ID,''),"
				+ "			 		COALESCE(PMB_TERMINAL_ID,''),"
				+ "			 		COALESCE(PMBP_TERMINAL_ID,'')"
				+ "			 ),',','^[0-9]+$') AS TERMINAL_ID,"
				+ "			 USER_TYPE,"
				+ "			 USER_ID,"
				+ "			 FORM_ID,"
				+ "			 LRT_ID,"
				+ "			 SALE_MODE,"
				+ "			 BABY_GENDER,"
				+ "			 BABY_BIRTHDAY,"
				+ "			 INSURE_TYPE,"
				+ "			 SEX,"
				+ "			 BABY_RELATION,"
				+ "			 P_BIRTHDAY,"
				+ "			 PAY_TYPE,"
				+ "			 PAY_PERIOD,"
				+ "			 PH_NAME,"
				+ "			 PH_BIRTHDAY,"
				+ "			 PH_EMAIL,"
				+ "			 PH_INDUSTRY,"
				+ "			 PH_WORKTYPE,"
				+ "			 INS_RELATION,"
				+ "			 INS_NAME,"
				+ "			 INS_BIRTHDAY,"
				+ "			 BENE_RELATION,"
				+ "			 BENE_NAME,"
				+ "			 BENE_BIRTHDAY,"
				+ "			 CONCAT(TPG_EVENT,"
				+ "			 		TBN_SUBTYPE,"
				+ "					PSPG_EVENT,"
				+ "					PSBTN_SUBTYPE,"
				+ "				    HIP_EVENT,"
				+ "				    HIB_SUBTYPE,"
				+ "					PMP_EVENT,"
				+ "				    PMB_SUBTYPE,"
				+ "					PMBP_EVENT"
				+ "			 ) AS LOCUS,"
				+ "			 SUCCESS,"
				+ "			 LAST_STEP,"
				+ "			 START_TIME,"
				+ "			 END_TIME,"
				+ "			 FROM_ID"
				+ "	 FROM    TMP_FRMIDNULL"
				+ "	 WHERE   USER_ID <> ''"
				+ "		AND  USER_ID IS NOT NULL) FDN"
				+ "	GROUP BY FDN.APP_TYPE,"
				+ "			 FDN.APP_ID,"
				+ "			 FDN.TERMINAL_ID,"
				+ "			 FDN.USER_TYPE,"
				+ "			 FDN.USER_ID,"
				+ "			 FDN.FORM_ID,"
				+ "			 FDN.LRT_ID,"
				+ "			 FDN.SALE_MODE,"
				+ "			 FDN.BABY_GENDER,"
				+ "			 FDN.BABY_BIRTHDAY,"
				+ "			 FDN.INSURE_TYPE,"
				+ "			 FDN.SEX,"
				+ "			 FDN.BABY_RELATION,"
				+ "			 FDN.P_BIRTHDAY,"
				+ "			 FDN.PAY_TYPE,"
				+ "			 FDN.PAY_PERIOD,"
				+ "			 FDN.PH_NAME,"
				+ "			 FDN.PH_BIRTHDAY,"
				+ "			 FDN.PH_EMAIL,"
				+ "			 FDN.PH_INDUSTRY,"
				+ "			 FDN.PH_WORKTYPE,"
				+ "			 FDN.INS_RELATION,"
				+ "			 FDN.INS_NAME,"
				+ "			 FDN.INS_BIRTHDAY,"
				+ "			 FDN.BENE_RELATION,"
				+ "			 FDN.BENE_NAME,"
				+ "			 FDN.BENE_BIRTHDAY,"
				+ "			 FDN.LOCUS,"
				+ "			 FDN.SUCCESS,"
				+ "			 FDN.LAST_STEP,"
				+ "			 FDN.FROM_ID ";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_tuserId");
	}

	/**
	 * 
	 * @Description: 合并formId为空的数据
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月27日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame combineByFormIdNull(HiveContext sqlContext) {
		String sql = "SELECT TPG.APP_TYPE,"
				+ "			 TPG.APP_ID,"
				+ "			 TPG.TERMINAL_ID AS TPG_TERMINAL_ID,"
				+ "			 TBN.TERMINAL_ID AS TBN_TERMINAL_ID,"
				+ "			 PSPG.TERMINAL_ID AS PSPG_TERMINAL_ID,"
				+ "			 PSBTN.TERMINAL_ID AS PSBTN_TERMINAL_ID,"
				+ "			 HIP.TERMINAL_ID AS HIP_TERMINAL_ID,"
				+ "			 HIB.TERMINAL_ID AS HIB_TERMINAL_ID,"
				+ "			 PMP.TERMINAL_ID AS PMP_TERMINAL_ID,"
				+ "			 PMB.TERMINAL_ID AS PMB_TERMINAL_ID,"
				+ "			 PMBP.TERMINAL_ID AS PMBP_TERMINAL_ID,"
				+ "			 'MEM' AS USER_TYPE,"
				+ "			 CASE WHEN PMBP.USER_ID IS NOT NULL THEN PMBP.USER_ID"
				+ "				  WHEN PMB.USER_ID IS NOT NULL THEN PMB.USER_ID"
				+ "				  WHEN PMP.USER_ID IS NOT NULL THEN PMP.USER_ID"
				+ "				  WHEN HIB.USER_ID IS NOT NULL THEN HIB.USER_ID"
				+ "				  WHEN HIP.USER_ID IS NOT NULL THEN HIP.USER_ID"
				+ "				  WHEN PSBTN.USER_ID IS NOT NULL THEN PSBTN.USER_ID"
				+ "				  WHEN PSPG.USER_ID IS NOT NULL THEN PSPG.USER_ID"
				+ "				  WHEN TBN.USER_ID IS NOT NULL THEN TBN.USER_ID"
				+ "				  WHEN TPG.USER_ID IS NOT NULL THEN TPG.USER_ID"
				+ "				  ELSE ''"
				+ "			 END AS USER_ID,"
				+ "			 TPG.FORM_ID,"
				+ "			 TPG.LRT_ID,"
				+ "			 CASE WHEN PMBP.USER_ID IS NOT NULL THEN PMBP.SALE_MODE"
				+ "				  WHEN PMB.USER_ID IS NOT NULL THEN PMB.SALE_MODE"
				+ "				  WHEN PMP.USER_ID IS NOT NULL THEN PMP.SALE_MODE"
				+ "				  WHEN HIB.USER_ID IS NOT NULL THEN HIB.SALE_MODE"
				+ "				  WHEN HIP.USER_ID IS NOT NULL THEN HIP.SALE_MODE"
				+ "				  WHEN PSBTN.USER_ID IS NOT NULL THEN PSBTN.SALE_MODE"
				+ "				  WHEN PSPG.USER_ID IS NOT NULL THEN PSPG.SALE_MODE"
				+ "				  WHEN TBN.USER_ID IS NOT NULL THEN TBN.SALE_MODE"
				+ "				  WHEN TPG.USER_ID IS NOT NULL THEN TPG.SALE_MODE"
				+ "				  ELSE ''"
				+ "			 END AS SALE_MODE,"
				+ "			 TBN.BABY_GENDER,"
				+ "			 TBN.BABY_BIRTHDAY,"
				+ "			 TBN.INSURE_TYPE,"
				+ "			 TBN.SEX,"
				+ "			 TBN.BABY_RELATION,"
				+ "			 TBN.P_BIRTHDAY,"
				+ "			 TBN.PAY_TYPE,"
				+ "			 TBN.PAY_PERIOD,"
				+ "			 PSBTN.PH_NAME,"
				+ "			 PSBTN.PH_BIRTHDAY,"
				+ "			 PSBTN.PH_EMAIL,"
				+ "			 PSBTN.PH_INDUSTRY,"
				+ "			 PSBTN.PH_WORKTYPE,"
				+ "			 PSBTN.INS_RELATION,"
				+ "			 PSBTN.INS_NAME,"
				+ "			 PSBTN.INS_BIRTHDAY,"
				+ "			 PSBTN.BENE_RELATION,"
				+ "			 PSBTN.BENE_NAME,"
				+ "			 PSBTN.BENE_BIRTHDAY,"
				+ "			 PMBP.SUCCESS,"
				+ "			 TPG.EVENT AS TPG_EVENT,"
				+ "			 CASE WHEN TBN.SUBTYPE IS NULL THEN ''"
				+ "				  ELSE CONCAT('->',TBN.EVENT,'.',TBN.SUBTYPE)"
				+ "			 END AS TBN_SUBTYPE,"
				+ "			 CASE WHEN PSPG.EVENT IS NULL THEN ''"
				+ "				  ELSE CONCAT('->',PSPG.EVENT)"
				+ "			 END AS PSPG_EVENT,"
				+ "			 CASE WHEN PSBTN.SUBTYPE IS NULL THEN ''"
				+ "				  ELSE CONCAT('->',PSBTN.EVENT,'.',PSBTN.SUBTYPE)"
				+ "			 END AS PSBTN_SUBTYPE,"
				+ "			 CASE WHEN HIP.EVENT IS NULL THEN ''"
				+ "				  ELSE CONCAT('->',HIP.EVENT)"
				+ "			 END AS HIP_EVENT,"
				+ "			 CASE WHEN HIB.SUBTYPE IS NULL THEN ''"
				+ "				  ELSE CONCAT('->',HIB.EVENT,'.',HIB.SUBTYPE)"
				+ "			 END AS HIB_SUBTYPE,"
				+ "			 CASE WHEN PMP.EVENT IS NULL THEN ''"
				+ "				  ELSE CONCAT('->',PMP.EVENT)"
				+ "			 END AS PMP_EVENT,"
				+ "			 CASE WHEN PMB.SUBTYPE IS NULL THEN ''"
				+ "				  ELSE CONCAT('->',PMB.EVENT,'.',PMB.SUBTYPE)"
				+ "			 END AS PMB_SUBTYPE,"
				+ "			 CASE WHEN PMBP.EVENT IS NULL THEN ''"
				+ "				  ELSE CONCAT('->',PMBP.EVENT)"
				+ "			 END AS PMBP_EVENT,"
				+ "			CASE WHEN PMBP.EVENT IS NOT NULL THEN PMBP.EVENT"
				+ "				 WHEN PMB.SUBTYPE IS NOT NULL THEN CONCAT(PMB.EVENT,'.',PMB.SUBTYPE)"
				+ "				 WHEN PMP.EVENT IS NOT NULL THEN PMP.EVENT"
				+ "				 WHEN HIB.SUBTYPE IS NOT NULL THEN CONCAT(HIB.EVENT,'.',HIB.SUBTYPE)"
				+ "				 WHEN HIP.EVENT IS NOT NULL THEN HIP.EVENT"
				+ "				 WHEN PSBTN.SUBTYPE IS NOT NULL THEN CONCAT(PSBTN.EVENT,'',PSBTN.SUBTYPE)"
				+ "				 WHEN PSPG.EVENT IS NOT NULL THEN PSPG.EVENT"
				+ "				 WHEN TBN.SUBTYPE IS NOT NULL THEN CONCAT(TBN.EVENT,'.',TBN.SUBTYPE)"
				+ "				 WHEN TPG.EVENT IS NOT NULL THEN TPG.EVENT"
				+ "				 ELSE ''"
				+ "			END AS LAST_STEP,"
				+ "			TPG.VISIT_TIME AS START_TIME,"
				+ "			CASE WHEN PMBP.EVENT IS NOT NULL THEN PMBP.VISIT_TIME"
				+ "				 WHEN PMB.SUBTYPE IS NOT NULL THEN PMB.VISIT_TIME"
				+ "				 WHEN PMP.EVENT IS NOT NULL THEN PMP.VISIT_TIME"
				+ "				 WHEN HIB.SUBTYPE IS NOT NULL THEN HIB.VISIT_TIME"
				+ "				 WHEN HIP.EVENT IS NOT NULL THEN HIP.VISIT_TIME"
				+ "				 WHEN PSBTN.SUBTYPE IS NOT NULL THEN PSBTN.VISIT_TIME"
				+ "				 WHEN PSPG.EVENT IS NOT NULL THEN PSPG.VISIT_TIME"
				+ "				 WHEN TBN.SUBTYPE IS NOT NULL THEN TBN.VISIT_TIME"
				+ "				 WHEN TPG.EVENT IS NOT NULL THEN TPG.VISIT_TIME"
				+ "				 ELSE ''"
				+ "			END AS END_TIME,"
				+ "			TPG.FROM_ID"
				+ "	  FROM  (SELECT * "
				+ "			  FROM  TMP_PREMIUMTESTPAGE"
				+ "			 WHERE  FORM_ID = ''"
				+ "			    OR FORM_ID IS  NULL) TPG"
				+ "	LEFT JOIN (SELECT * "
				+ "			  FROM  TMP_PREMIUMTESTBTN"
				+ "			 WHERE  FORM_ID = ''"
				+ "			    OR FORM_ID IS  NULL) TBN"
				+ "	   ON   (CASE WHEN TPG.USER_ID <> '' AND TPG.USER_ID IS NOT NULL THEN TPG.USER_ID"
				+ "				  ELSE TPG.TERMINAL_ID"
				+ "			 END) = (CASE WHEN TBN.USER_ID <> '' AND TBN.USER_ID IS NOT NULL THEN TBN.USER_ID"
				+ "						  ELSE TBN.TERMINAL_ID END)"
				+ "		AND TPG.LRT_ID = TBN.LRT_ID"
				+ "	    AND CAST(TPG.VISIT_TIME AS BIGINT) <= CAST(TBN.VISIT_TIME AS BIGINT)"
				+ " LEFT JOIN (SELECT * "
				+ "			  FROM  TMP_PERSONINFOPAGE"
				+ "			 WHERE  FORM_ID = ''"
				+ "			    OR FORM_ID IS  NULL) PSPG"
				+ "	   ON   (CASE WHEN TPG.USER_ID <> '' AND TPG.USER_ID IS NOT NULL THEN TPG.USER_ID"
				+ "				  ELSE TPG.TERMINAL_ID"
				+ "			 END) = (CASE WHEN PSPG.USER_ID <> '' AND PSPG.USER_ID IS NOT NULL THEN PSPG.USER_ID"
				+ "						  ELSE PSPG.TERMINAL_ID END)"
				+ "		AND TPG.LRT_ID = PSPG.LRT_ID"
				+ "	    AND CAST(TPG.VISIT_TIME AS BIGINT) <= CAST(PSPG.VISIT_TIME AS BIGINT)"
				+ " LEFT JOIN (SELECT * "
				+ "			  FROM  TMP_PERSONINFOBTN"
				+ "			 WHERE  FORM_ID = ''"
				+ "			    OR FORM_ID IS  NULL) PSBTN"
				+ "	   ON   (CASE WHEN TPG.USER_ID <> '' AND TPG.USER_ID IS NOT NULL THEN TPG.USER_ID"
				+ "				  ELSE TPG.TERMINAL_ID"
				+ "			 END) = (CASE WHEN PSBTN.USER_ID <> '' AND PSBTN.USER_ID IS NOT NULL THEN PSBTN.USER_ID"
				+ "						  ELSE PSBTN.TERMINAL_ID END)"
				+ "		AND TPG.LRT_ID = PSBTN.LRT_ID"
				+ "	    AND CAST(TPG.VISIT_TIME AS BIGINT) <= CAST(PSBTN.VISIT_TIME AS BIGINT)"
				+ " LEFT JOIN (SELECT * "
				+ "			  FROM  TMP_HEALTHINFORMPAGE"
				+ "			 WHERE  FORM_ID = ''"
				+ "			    OR FORM_ID IS  NULL) HIP"
				+ "	   ON   (CASE WHEN TPG.USER_ID <> '' AND TPG.USER_ID IS NOT NULL THEN TPG.USER_ID"
				+ "				  ELSE TPG.TERMINAL_ID"
				+ "			 END) = (CASE WHEN HIP.USER_ID <> '' AND HIP.USER_ID IS NOT NULL THEN HIP.USER_ID"
				+ "						  ELSE HIP.TERMINAL_ID END)"
				+ "		AND TPG.LRT_ID = HIP.LRT_ID"
				+ "	    AND CAST(TPG.VISIT_TIME AS BIGINT) <= CAST(HIP.VISIT_TIME AS BIGINT)"
				+ " LEFT JOIN (SELECT * "
				+ "			  FROM  TMP_HEALTHINFORMBTN"
				+ "			 WHERE  FORM_ID = ''"
				+ "			    OR FORM_ID IS  NULL) HIB"
				+ "	   ON   (CASE WHEN TPG.USER_ID <> '' AND TPG.USER_ID IS NOT NULL THEN TPG.USER_ID"
				+ "				  ELSE TPG.TERMINAL_ID"
				+ "			 END) = (CASE WHEN HIB.USER_ID <> '' AND HIB.USER_ID IS NOT NULL THEN HIB.USER_ID"
				+ "						  ELSE HIB.TERMINAL_ID END)"
				+ "		AND TPG.LRT_ID = HIB.LRT_ID"
				+ "	    AND CAST(TPG.VISIT_TIME AS BIGINT) <= CAST(HIB.VISIT_TIME AS BIGINT)"
				+ " LEFT JOIN (SELECT * "
				+ "			  FROM  TMP_PAYMENTPAGE"
				+ "			 WHERE  FORM_ID = ''"
				+ "			    OR FORM_ID IS  NULL) PMP"
				+ "	   ON   (CASE WHEN TPG.USER_ID <> '' AND TPG.USER_ID IS NOT NULL THEN TPG.USER_ID"
				+ "				  ELSE TPG.TERMINAL_ID"
				+ "			 END) = (CASE WHEN PMP.USER_ID <> '' AND PMP.USER_ID IS NOT NULL THEN PMP.USER_ID"
				+ "						  ELSE PMP.TERMINAL_ID END)"
				+ "		AND TPG.LRT_ID = PMP.LRT_ID"
				+ "	    AND CAST(TPG.VISIT_TIME AS BIGINT) <= CAST(PMP.VISIT_TIME AS BIGINT)"
				+ " LEFT JOIN (SELECT * "
				+ "			  FROM  TMP_PAYMENTBTN"
				+ "			 WHERE  FORM_ID = ''"
				+ "			    OR FORM_ID IS  NULL) PMB"
				+ "	   ON   (CASE WHEN TPG.USER_ID <> '' AND TPG.USER_ID IS NOT NULL THEN TPG.USER_ID"
				+ "				  ELSE TPG.TERMINAL_ID"
				+ "			 END) = (CASE WHEN PMB.USER_ID <> '' AND PMB.USER_ID IS NOT NULL THEN PMB.USER_ID"
				+ "						  ELSE PMB.TERMINAL_ID END)"
				+ "		AND TPG.LRT_ID = PMB.LRT_ID"
				+ "	    AND CAST(TPG.VISIT_TIME AS BIGINT) <= CAST(PMB.VISIT_TIME AS BIGINT)"
				+ " LEFT JOIN (SELECT * "
				+ "			  FROM  TMP_PAYMENTBACKPAGE"
				+ "			 WHERE  FORM_ID = ''"
				+ "			    OR FORM_ID IS  NULL) PMBP"
				+ "	   ON   (CASE WHEN TPG.USER_ID <> '' AND TPG.USER_ID IS NOT NULL THEN TPG.USER_ID"
				+ "				  ELSE TPG.TERMINAL_ID"
				+ "			 END) = (CASE WHEN PMBP.USER_ID <> '' AND PMBP.USER_ID IS NOT NULL THEN PMBP.USER_ID"
				+ "						  ELSE PMBP.TERMINAL_ID END)"
				+ "		AND TPG.LRT_ID = PMBP.LRT_ID"
				+ "	    AND CAST(TPG.VISIT_TIME AS BIGINT) <= CAST(PMBP.VISIT_TIME AS BIGINT)";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_FrmIdNull");
	}

	/**
	 * 
	 * @Description: 合并formId不为空的数据
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月27日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame combineByFormIdNotNull(HiveContext sqlContext) {
		String sql = "SELECT TPG.APP_TYPE,"
				+ "			 TPG.APP_ID,"
				+ "			 TPG.TERMINAL_ID AS TPG_TERMINAL_ID,"
				+ "			 TBN.TERMINAL_ID AS TBN_TERMINAL_ID,"
				+ "			 PSPG.TERMINAL_ID AS PSPG_TERMINAL_ID,"
				+ "			 PSBTN.TERMINAL_ID AS PSBTN_TERMINAL_ID,"
				+ "			 HIP.TERMINAL_ID AS HIP_TERMINAL_ID,"
				+ "			 HIB.TERMINAL_ID AS HIB_TERMINAL_ID,"
				+ "			 PMP.TERMINAL_ID AS PMP_TERMINAL_ID,"
				+ "			 PMB.TERMINAL_ID AS PMB_TERMINAL_ID,"
				+ "			 PMBP.TERMINAL_ID AS PMBP_TERMINAL_ID,"
				+ "			 'MEM' AS USER_TYPE,"
				+ "			 CASE WHEN PMBP.USER_ID IS NOT NULL THEN PMBP.USER_ID"
				+ "				  WHEN PMB.USER_ID IS NOT NULL THEN PMB.USER_ID"
				+ "				  WHEN PMP.USER_ID IS NOT NULL THEN PMP.USER_ID"
				+ "				  WHEN HIB.USER_ID IS NOT NULL THEN HIB.USER_ID"
				+ "				  WHEN HIP.USER_ID IS NOT NULL THEN HIP.USER_ID"
				+ "				  WHEN PSBTN.USER_ID IS NOT NULL THEN PSBTN.USER_ID"
				+ "				  WHEN PSPG.USER_ID IS NOT NULL THEN PSPG.USER_ID"
				+ "				  WHEN TBN.USER_ID IS NOT NULL THEN TBN.USER_ID"
				+ "				  WHEN TPG.USER_ID IS NOT NULL THEN TPG.USER_ID"
				+ "				  ELSE ''"
				+ "			 END AS USER_ID,"
				+ "			 TPG.FORM_ID,"
				+ "			 TPG.LRT_ID,"
				+ "			 CASE WHEN PMBP.USER_ID IS NOT NULL THEN PMBP.SALE_MODE"
				+ "				  WHEN PMB.USER_ID IS NOT NULL THEN PMB.SALE_MODE"
				+ "				  WHEN PMP.USER_ID IS NOT NULL THEN PMP.SALE_MODE"
				+ "				  WHEN HIB.USER_ID IS NOT NULL THEN HIB.SALE_MODE"
				+ "				  WHEN HIP.USER_ID IS NOT NULL THEN HIP.SALE_MODE"
				+ "				  WHEN PSBTN.USER_ID IS NOT NULL THEN PSBTN.SALE_MODE"
				+ "				  WHEN PSPG.USER_ID IS NOT NULL THEN PSPG.SALE_MODE"
				+ "				  WHEN TBN.USER_ID IS NOT NULL THEN TBN.SALE_MODE"
				+ "				  WHEN TPG.USER_ID IS NOT NULL THEN TPG.SALE_MODE"
				+ "				  ELSE ''"
				+ "			 END AS SALE_MODE,"
				+ "			 TBN.BABY_GENDER,"
				+ "			 TBN.BABY_BIRTHDAY,"
				+ "			 TBN.INSURE_TYPE,"
				+ "			 TBN.SEX,"
				+ "			 TBN.BABY_RELATION,"
				+ "			 TBN.P_BIRTHDAY,"
				+ "			 TBN.PAY_TYPE,"
				+ "			 TBN.PAY_PERIOD,"
				+ "			 PSBTN.PH_NAME,"
				+ "			 PSBTN.PH_BIRTHDAY,"
				+ "			 PSBTN.PH_EMAIL,"
				+ "			 PSBTN.PH_INDUSTRY,"
				+ "			 PSBTN.PH_WORKTYPE,"
				+ "			 PSBTN.INS_RELATION,"
				+ "			 PSBTN.INS_NAME,"
				+ "			 PSBTN.INS_BIRTHDAY,"
				+ "			 PSBTN.BENE_RELATION,"
				+ "			 PSBTN.BENE_NAME,"
				+ "			 PSBTN.BENE_BIRTHDAY,"
				+ "			 PMBP.SUCCESS,"
				+ "			 TPG.EVENT AS TPG_EVENT,"
				+ "			 CASE WHEN TBN.SUBTYPE IS NULL THEN ''"
				+ "				  ELSE CONCAT('->',TBN.EVENT,'.',TBN.SUBTYPE)"
				+ "			 END AS TBN_SUBTYPE,"
				+ "			 CASE WHEN PSPG.EVENT IS NULL THEN ''"
				+ "				  ELSE CONCAT('->',PSPG.EVENT)"
				+ "			 END AS PSPG_EVENT,"
				+ "			 CASE WHEN PSBTN.SUBTYPE IS NULL THEN ''"
				+ "				  ELSE CONCAT('->',PSBTN.EVENT,'.',PSBTN.SUBTYPE)"
				+ "			 END AS PSBTN_SUBTYPE,"
				+ "			 CASE WHEN HIP.EVENT IS NULL THEN ''"
				+ "				  ELSE CONCAT('->',HIP.EVENT)"
				+ "			 END AS HIP_EVENT,"
				+ "			 CASE WHEN HIB.SUBTYPE IS NULL THEN ''"
				+ "				  ELSE CONCAT('->',HIB.EVENT,'.',HIB.SUBTYPE)"
				+ "			 END AS HIB_SUBTYPE,"
				+ "			 CASE WHEN PMP.EVENT IS NULL THEN ''"
				+ "				  ELSE CONCAT('->',PMP.EVENT)"
				+ "			 END AS PMP_EVENT,"
				+ "			 CASE WHEN PMB.SUBTYPE IS NULL THEN ''"
				+ "				  ELSE CONCAT('->',PMB.EVENT,'.',PMB.SUBTYPE)"
				+ "			 END AS PMB_SUBTYPE,"
				+ "			 CASE WHEN PMBP.EVENT IS NULL THEN ''"
				+ "				  ELSE CONCAT('->',PMBP.EVENT)"
				+ "			 END AS PMBP_EVENT,"
				+ "			CASE WHEN PMBP.EVENT IS NOT NULL THEN PMBP.EVENT"
				+ "				 WHEN PMB.SUBTYPE IS NOT NULL THEN CONCAT(PMB.EVENT,'.',PMB.SUBTYPE)"
				+ "				 WHEN PMP.EVENT IS NOT NULL THEN PMP.EVENT"
				+ "				 WHEN HIB.SUBTYPE IS NOT NULL THEN CONCAT(HIB.EVENT,'.',HIB.SUBTYPE)"
				+ "				 WHEN HIP.EVENT IS NOT NULL THEN HIP.EVENT"
				+ "				 WHEN PSBTN.SUBTYPE IS NOT NULL THEN CONCAT(PSBTN.EVENT,'',PSBTN.SUBTYPE)"
				+ "				 WHEN PSPG.EVENT IS NOT NULL THEN PSPG.EVENT"
				+ "				 WHEN TBN.SUBTYPE IS NOT NULL THEN CONCAT(TBN.EVENT,'.',TBN.SUBTYPE)"
				+ "				 WHEN TPG.EVENT IS NOT NULL THEN TPG.EVENT"
				+ "				 ELSE ''"
				+ "			END AS LAST_STEP,"
				+ "			TPG.VISIT_TIME AS START_TIME,"
				+ "			CASE WHEN PMBP.EVENT IS NOT NULL THEN PMBP.VISIT_TIME"
				+ "				 WHEN PMB.SUBTYPE IS NOT NULL THEN PMB.VISIT_TIME"
				+ "				 WHEN PMP.EVENT IS NOT NULL THEN PMP.VISIT_TIME"
				+ "				 WHEN HIB.SUBTYPE IS NOT NULL THEN HIB.VISIT_TIME"
				+ "				 WHEN HIP.EVENT IS NOT NULL THEN HIP.VISIT_TIME"
				+ "				 WHEN PSBTN.SUBTYPE IS NOT NULL THEN PSBTN.VISIT_TIME"
				+ "				 WHEN PSPG.EVENT IS NOT NULL THEN PSPG.VISIT_TIME"
				+ "				 WHEN TBN.SUBTYPE IS NOT NULL THEN TBN.VISIT_TIME"
				+ "				 WHEN TPG.EVENT IS NOT NULL THEN TPG.VISIT_TIME"
				+ "				 ELSE ''"
				+ "			END AS END_TIME,"
				+ "			TPG.FROM_ID"
				+ "	  FROM  (SELECT * "
				+ "			  FROM  TMP_PREMIUMTESTPAGE"
				+ "			 WHERE  FORM_ID <> ''"
				+ "			    AND FORM_ID IS NOT NULL) TPG"
				+ "	LEFT JOIN (SELECT * "
				+ "			   FROM TMP_PREMIUMTESTBTN"
				+ "			   WHERE  FORM_ID <> ''"
				+ "			    AND FORM_ID IS NOT NULL) TBN"
				+ "	   ON   TPG.FORM_ID = TBN.FORM_ID"
				+ "		AND CAST(TPG.VISIT_TIME AS BIGINT) <= CAST(TBN.VISIT_TIME AS BIGINT)"
				+ " LEFT JOIN (SELECT * "
				+ "			   FROM TMP_PERSONINFOPAGE"
				+ "			   WHERE  FORM_ID <> ''"
				+ "			    AND FORM_ID IS NOT NULL) PSPG"
				+ "	   ON   TPG.FORM_ID = PSPG.FORM_ID"
				+ "	    AND CAST(TPG.VISIT_TIME AS BIGINT) <= CAST(PSPG.VISIT_TIME AS BIGINT)"
				+ " LEFT JOIN (SELECT * "
				+ "			   FROM TMP_PERSONINFOBTN"
				+ "			   WHERE  FORM_ID <> ''"
				+ "			    AND FORM_ID IS NOT NULL) PSBTN"
				+ "	   ON   TPG.FORM_ID = PSBTN.FORM_ID"
				+ "	    AND CAST(TPG.VISIT_TIME AS BIGINT) <= CAST(PSBTN.VISIT_TIME AS BIGINT)"
				+ " LEFT JOIN (SELECT * "
				+ "			   FROM TMP_HEALTHINFORMPAGE"
				+ "			   WHERE  FORM_ID <> ''"
				+ "			    AND FORM_ID IS NOT NULL) HIP"
				+ "	   ON   TPG.FORM_ID = HIP.FORM_ID"
				+ "	    AND CAST(TPG.VISIT_TIME AS BIGINT) <= CAST(HIP.VISIT_TIME AS BIGINT)"
				+ " LEFT JOIN (SELECT * "
				+ "			   FROM TMP_HEALTHINFORMBTN"
				+ "			   WHERE  FORM_ID <> ''"
				+ "			    AND FORM_ID IS NOT NULL) HIB"
				+ "	   ON   TPG.FORM_ID = HIB.FORM_ID"
				+ "	    AND CAST(TPG.VISIT_TIME AS BIGINT) <= CAST(HIB.VISIT_TIME AS BIGINT)"
				+ " LEFT JOIN (SELECT * "
				+ "			   FROM TMP_PAYMENTPAGE"
				+ "			   WHERE  FORM_ID <> ''"
				+ "			    AND FORM_ID IS NOT NULL) PMP"
				+ "	   ON   TPG.FORM_ID = PMP.FORM_ID"
				+ "	    AND CAST(TPG.VISIT_TIME AS BIGINT) <= CAST(PMP.VISIT_TIME AS BIGINT)"
				+ " LEFT JOIN (SELECT * "
				+ "			   FROM TMP_PAYMENTBTN"
				+ "			   WHERE  FORM_ID <> ''"
				+ "			    AND FORM_ID IS NOT NULL) PMB"
				+ "	   ON   TPG.FORM_ID = PMB.FORM_ID"
				+ "	    AND CAST(TPG.VISIT_TIME AS BIGINT) <= CAST(PMB.VISIT_TIME AS BIGINT)"
				+ " LEFT JOIN (SELECT * "
				+ "			   FROM TMP_PAYMENTBACKPAGE"
				+ "			   WHERE  FORM_ID <> ''"
				+ "			    AND FORM_ID IS NOT NULL) PMBP"
				+ "	   ON   TPG.FORM_ID = PMBP.FORM_ID"
				+ "	    AND CAST(TPG.VISIT_TIME AS BIGINT) <= CAST(PMBP.VISIT_TIME AS BIGINT)";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_FrmIdNotNull");
	}


	/**
	 * 
	 * @Description: 补全原数中为空的form_id
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月26日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame addFormId2Src(HiveContext sqlContext) {
		String sql = "SELECT NRSTID.APP_TYPE,"
				+ "			 NRSTID.APP_ID,"
				+ "			 NRSTID.EVENT,"
				+ "			 NRSTID.SUBTYPE,"
				+ "			 NRSTID.TERMINAL_ID,"
				+ "			 NRSTID.USER_ID,"
				+ "			 NRSTID.VISIT_TIME,"
				+ "			 NRSTID.FROM_ID,"
				+ "			 NRSTID.LRT_ID,"
				+ "			 NRSTID.SALE_MODE,"
				+ "			 NRSTID.BABY_GENDER,"
				+ "			 NRSTID.BABY_BIRTHDAY,"
				+ "			 NRSTID.INSURE_TYPE,"
				+ "			 NRSTID.SEX,"
				+ "			 NRSTID.BABY_RELATION,"
				+ "			 NRSTID.P_BIRTHDAY,"
				+ "			 NRSTID.PAY_TYPE,"
				+ "			 NRSTID.PAY_PERIOD,"
				+ "			 NRSTID.PH_NAME,"
				+ "			 NRSTID.PH_BIRTHDAY,"
				+ "			 NRSTID.PH_EMAIL,"
				+ "			 NRSTID.PH_INDUSTRY,"
				+ "			 NRSTID.PH_WORKTYPE,"
				+ "			 NRSTID.INS_RELATION,"
				+ "			 NRSTID.INS_NAME,"
				+ "			 NRSTID.INS_BIRTHDAY,"
				+ "			 NRSTID.BENE_RELATION,"
				+ "			 NRSTID.BENE_NAME,"
				+ "			 NRSTID.BENE_BIRTHDAY,"
				+ "			 NRSTID.SUCCESS,"
				+ "			 CASE WHEN NRSTID.FORM_ID = '' OR NRSTID.FORM_ID IS NULL THEN COALESCE(FRMMAP.FORM_ID,'')"
				+ "				  ELSE NRSTID.FORM_ID"
				+ "			 END AS FORM_ID"
				+ "	   FROM  TMP_NEARSTFRMID NRSTID"
				+ "	LEFT JOIN TMP_FORMIDMAP FRMMAP"
				+ "	    ON   NRSTID.TERMINAL_ID = FRMMAP.TERMINAL_ID"
				+ "		AND  NRSTID.USER_ID = FRMMAP.USER_ID"
				+ "		AND  NRSTID.LRT_ID = FRMMAP.LRT_ID"
				+ "		AND  CAST(NRSTID.FORM_DATE AS BIGINT) = CAST(FRMMAP.FORM_DATE AS BIGINT)";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_frmId2Src",DataFrameUtil.CACHETABLE_PARQUET);
	}


	/**
	 * 
	 * @Description: 获取最近的formId
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月26日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getNearstFormID(HiveContext sqlContext) {
		String sql = "SELECT ALSRC.APP_TYPE,"
				+ "			 ALSRC.APP_ID,"
				+ "			 ALSRC.EVENT,"
				+ "			 ALSRC.SUBTYPE,"
				+ "			 ALSRC.TERMINAL_ID,"
				+ "			 ALSRC.USER_ID,"
				+ "			 ALSRC.VISIT_TIME,"
				+ "			 ALSRC.FROM_ID,"
				+ "			 ALSRC.LRT_ID,"
				+ "			 ALSRC.SALE_MODE,"
				+ "			 ALSRC.BABY_GENDER,"
				+ "			 ALSRC.BABY_BIRTHDAY,"
				+ "			 ALSRC.INSURE_TYPE,"
				+ "			 ALSRC.SEX,"
				+ "			 ALSRC.BABY_RELATION,"
				+ "			 ALSRC.P_BIRTHDAY,"
				+ "			 ALSRC.PAY_TYPE,"
				+ "			 ALSRC.PAY_PERIOD,"
				+ "			 ALSRC.PH_NAME,"
				+ "			 ALSRC.PH_BIRTHDAY,"
				+ "			 ALSRC.PH_EMAIL,"
				+ "			 ALSRC.PH_INDUSTRY,"
				+ "			 ALSRC.PH_WORKTYPE,"
				+ "			 ALSRC.INS_RELATION,"
				+ "			 ALSRC.INS_NAME,"
				+ "			 ALSRC.INS_BIRTHDAY,"
				+ "			 ALSRC.BENE_RELATION,"
				+ "			 ALSRC.BENE_NAME,"
				+ "			 ALSRC.BENE_BIRTHDAY,"
				+ "			 ALSRC.SUCCESS,"
				+ "			 CASE WHEN EVENT = '保费测算' THEN MIN("
				+ "				 			CASE WHEN FRMMAP.FORM_DATE IS NULL OR FRMMAP.FORM_DATE = '' THEN 0"
				+ "				 	  			 ELSE CAST(FRMMAP.FORM_DATE AS BIGINT)"
				+ "				 			END)"
				+ "				  WHEN EVENT <> '保费测算' THEN MAX(CASE WHEN FRMMAP.FORM_DATE IS NULL OR FRMMAP.FORM_DATE = '' THEN 0"
				+ "				 	  			 ELSE CAST(FRMMAP.FORM_DATE AS BIGINT)"
				+ "				 			END)"
				+ "				  ELSE 0"
				+ "			 END AS FORM_DATE,"
				+ "			 ALSRC.FORM_ID"
				+ "	   FROM  Tmp_userIdAll ALSRC"
				+ "	LEFT JOIN TMP_FORMIDMAP FRMMAP"
				+ "	    ON   ALSRC.TERMINAL_ID = FRMMAP.TERMINAL_ID"
				+ "		AND  ALSRC.USER_ID = FRMMAP.USER_ID"
				+ "		AND  ALSRC.LRT_ID = FRMMAP.LRT_ID"
				+ "		AND  (CASE WHEN ALSRC.EVENT = '保费测算' THEN DATEDIFF(FROM_UNIXTIME(CAST(ALSRC.VISIT_TIME/1000 AS BIGINT),'yyyy-MM-dd'), FROM_UNIXTIME(CAST(FRMMAP.FORM_DATE/1000 AS BIGINT),'yyyy-MM-dd')) "
				+ "				  ELSE 0"
				+ "			 END) = 0"
				+ "		AND  (CASE WHEN ALSRC.EVENT <> '保费测算' THEN CAST(ALSRC.VISIT_TIME AS BIGINT)-CAST(FRMMAP.FORM_DATE AS BIGINT)"
				+ "				   WHEN ALSRC.EVENT = '保费测算' THEN CAST(FRMMAP.FORM_DATE AS BIGINT)-CAST(ALSRC.VISIT_TIME AS BIGINT)"
				+ "				   ELSE 0"
				+ "			 END) >= 0"
				+ "	GROUP BY ALSRC.APP_TYPE,"
				+ "			 ALSRC.APP_ID,"
				+ "			 ALSRC.EVENT,"
				+ "			 ALSRC.SUBTYPE,"
				+ "			 ALSRC.TERMINAL_ID,"
				+ "			 ALSRC.USER_ID,"
				+ "			 ALSRC.VISIT_TIME,"
				+ "			 ALSRC.FROM_ID,"
				+ "			 ALSRC.LRT_ID,"
				+ "			 ALSRC.SALE_MODE,"
				+ "			 ALSRC.BABY_GENDER,"
				+ "			 ALSRC.BABY_BIRTHDAY,"
				+ "			 ALSRC.INSURE_TYPE,"
				+ "			 ALSRC.SEX,"
				+ "			 ALSRC.BABY_RELATION,"
				+ "			 ALSRC.P_BIRTHDAY,"
				+ "			 ALSRC.PAY_TYPE,"
				+ "			 ALSRC.PAY_PERIOD,"
				+ "			 ALSRC.PH_NAME,"
				+ "			 ALSRC.PH_BIRTHDAY,"
				+ "			 ALSRC.PH_EMAIL,"
				+ "			 ALSRC.PH_INDUSTRY,"
				+ "			 ALSRC.PH_WORKTYPE,"
				+ "			 ALSRC.INS_RELATION,"
				+ "			 ALSRC.INS_NAME,"
				+ "			 ALSRC.INS_BIRTHDAY,"
				+ "			 ALSRC.BENE_RELATION,"
				+ "			 ALSRC.BENE_NAME,"
				+ "			 ALSRC.BENE_BIRTHDAY,"
				+ "			 ALSRC.SUCCESS,"
				+ "			 ALSRC.FORM_ID";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_nearstfrmId");
	}

	/**
	 * 
	 * @Description: 生成订单映射表
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月26日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getFormMap(HiveContext sqlContext) {
		String sql = "SELECT ALSRC.TERMINAL_ID,"
				+ "			 ALSRC.USER_ID,"
				+ "			 ALSRC.LRT_ID,"
				+ "			 ALSRC.FORM_ID,"
				+ "			 CAST(UNIX_TIMESTAMP(COALESCE(APYFM.FORM_DATE,'2100-01-01 00:00:00'),'yyyy-MM-dd HH:mm:ss')*1000 AS STRING) AS FORM_DATE"
				+ "	   FROM  (SELECT TERMINAL_ID,"
				+ "					 USER_ID,"
				+ "					 LRT_ID,"
				+ "				     FORM_ID"
				+ "			   FROM  Tmp_userIdAll"
				+ "			  WHERE  FORM_ID <> ''"
				+ "				AND  FORM_ID IS NOT NULL"
				+ "			 GROUP BY TERMINAL_ID,"
				+ "					  USER_ID,"
				+ "					  LRT_ID,"
				+ "					  FORM_ID) ALSRC"
				+ "	LEFT JOIN P_APPLYFORM APYFM"
				+ "	   ON 	 ALSRC.FORM_ID = APYFM.FORM_ID";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_formIdMap",DataFrameUtil.CACHETABLE_PARQUET);
	}

	/**
	 * 
	 * @Description: 分解线索类型：1.少儿险；0.其他类型
	 * @param sqlContext
	 * @param config
	 * @return
	 * @author moyunqing
	 * @date 2016年9月24日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getClueType(HiveContext sqlContext, Map<String,Map<String,String>> config) {
		String sql = "SELECT DISTINCT"
				+ "			 * ,"
				+ 		 	 getClueTypeCol(config)
				+ "	   FROM  TMP_STATUS";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "FACT_INSURANCEPROCESS_EVENT");
	}

	/**
	 * 
	 * @Description: 根据lrtid配置动态拼sql
	 * @param config
	 * @return
	 * @author moyunqing
	 * @date 2016年9月24日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private String getClueTypeCol(Map<String, Map<String, String>> config) {
		String sql = "";
		if(config != null && !config.isEmpty()){
			Map<String,String> attr = null;
			String sign = null;
			String lrtid = null;
			for(String type : config.keySet()){
				attr = config.get(type);
				if(attr != null && !attr.isEmpty()){
					sign = attr.get("sign");
					lrtid = attr.get("lrtid");
					if(sign != null && lrtid != null){
						sql += " WHEN LRT_ID IN (" + lrtid + ") THEN '" + sign + "'";
					}
				}
			}
			if(StringUtils.isNotBlank(sql)){
				sql = "CASE " + sql + " ELSE '0' END AS CLUE_TYPE";
			}
		}
		if(StringUtils.isBlank(sql))
			sql = " '0' AS CLUE_TYPE";
		return sql;
	}

	/**
	 * 
	 * @Description: 解析本次投保是否成功
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月24日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getLocusAndStatus(HiveContext sqlContext) {
		//k订单表 
		//status 0-失败 1-成功
		String sql = "SELECT TCR.APP_TYPE,"
				+ "			 TCR.APP_ID,"
				+ "			 TCR.TERMINAL_ID,"
				+ "			 TCR.USER_TYPE,"
				+ "			 TCR.USER_ID,"
				+ "			 CASE WHEN APYF.LIA_POLICYNO <> '' AND APYF.LIA_POLICYNO IS NOT NULL THEN '1'"
				+ "				  ELSE '0'"
				+ "			 END AS STATUS,"
				+ "			 CASE WHEN APYF.LIA_POLICYNO IS NOT NULL AND TCR.SUCCESS = 'false' THEN '支付页面返回失败，但实际支付成功！'"
				+ "				  WHEN TCR.SUCCESS = 'false' THEN '支付失败'"
				+ "			 	  ELSE ''"
				+ "			 END AS INTERRUPT_REASON,"
				+ "			 TCR.FORM_ID,"
				+ "			 TCR.LRT_ID,"
				+ "			 TCR.LRT_NAME,"
				+ "			 TCR.SALE_MODE,"
				+ "			 TCR.BABY_GENDER,"
				+ "			 TCR.BABY_BIRTHDAY,"
				+ "			 TCR.INSURE_TYPE,"
				+ "			 TCR.SEX,"
				+ "			 TCR.BABY_RELATION,"
				+ "			 TCR.P_BIRTHDAY,"
				+ "			 TCR.PAY_TYPE,"
				+ "			 TCR.PAY_PERIOD,"
				+ "			 TCR.PH_NAME,"
				+ "			 TCR.PH_BIRTHDAY,"
				+ "			 TCR.PH_EMAIL,"
				+ "			 TCR.PH_INDUSTRY,"
				+ "			 TCR.PH_WORKTYPE,"
				+ "			 TCR.INS_RELATION,"
				+ "			 TCR.INS_NAME,"
				+ "			 TCR.INS_BIRTHDAY,"
				+ "			 TCR.BENE_RELATION,"
				+ "			 TCR.BENE_NAME,"
				+ "			 TCR.BENE_BIRTHDAY,"
				+ "			 TCR.SUCCESS,"
				+ "			 TCR.LOCUS,"
				+ "			 TCR.LAST_STEP,"
				+ "			 TCR.START_TIME,"
				+ "			 TCR.END_TIME,"
				+ "			 TCR.FROM_ID,"
				+ "			 CASE WHEN REGEXP_EXTRACT(APYF.FORM_RECORD,'<投保人.*移动电话=\"([0-9]*)\".*></投保人>') <> '' AND"
				+ "					   REGEXP_EXTRACT(APYF.FORM_RECORD,'<投保人.*移动电话=\"([0-9]*)\".*></投保人>') IS NOT NULL THEN"
				+ "					   REGEXP_EXTRACT(APYF.FORM_RECORD,'<投保人.*移动电话=\"([0-9]*)\".*></投保人>')"
				+ "			      WHEN REGEXP_EXTRACT(APYF.FORM_RECORD,'<投保人.*联系电话=\"([0-9]*)\".*></投保人>') <> '' AND "
				+ "					   REGEXP_EXTRACT(APYF.FORM_RECORD,'<投保人.*联系电话=\"([0-9]*)\".*></投保人>') IS NOT NULL THEN"
				+ "					   REGEXP_EXTRACT(APYF.FORM_RECORD,'<投保人.*联系电话=\"([0-9]*)\".*></投保人>')"
				+ "				  ELSE ''"
				+ "			END AS MOBILE"
				+ "	   FROM  Tmp_fulllrtname TCR"
				+ "	LEFT JOIN P_APPLYFORM APYF"
				+ "	   ON	 TCR.FORM_ID = APYF.FORM_ID";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_status");
	}

	/**
	 * 
	 * @Description: 合并投保流程各阶段
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月23日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getCombineRecord(HiveContext sqlContext) {
		String sql = "SELECT * FROM TMP_TUSERID"
				+ "	   UNION ALL "
				+ "	  SELECT * FROM TMP_TFORMID";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_combineRecord");
	}
	
	/**
	 * 
	 * @Description: 获取个人信息页面点击个人下一步按钮数据
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月24日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getPersonInfoBtn(HiveContext sqlContext) {
		String sql = "SELECT PIB_ALL.APP_TYPE,"
				+ "			 PIB_ALL.APP_ID,"
				+ "			 PIB_ALL.EVENT,"
				+ "			 PIB_ALL.SUBTYPE,"
				+ "			 PIB_MAX.TERMINAL_ID,"
				+ "			 PIB_ALL.USER_ID,"
				+ "			 PIB_ALL.VISIT_TIME,"
				+ "			 PIB_ALL.FROM_ID,"
				+ "			 PIB_ALL.LRT_ID,"
				+ "			 PIB_ALL.SALE_MODE,"
				+ "			 PIB_ALL.BABY_GENDER,"
				+ "			 PIB_ALL.BABY_BIRTHDAY,"
				+ "			 PIB_ALL.INSURE_TYPE,"
				+ "			 PIB_ALL.SEX,"
				+ "			 PIB_ALL.BABY_RELATION,"
				+ "			 PIB_ALL.P_BIRTHDAY,"
				+ "			 PIB_ALL.PAY_TYPE,"
				+ "			 PIB_ALL.PAY_PERIOD,"
				+ "			 PIB_ALL.PH_NAME,"
				+ "			 PIB_ALL.PH_BIRTHDAY,"
				+ "			 PIB_ALL.PH_EMAIL,"
				+ "			 PIB_ALL.PH_INDUSTRY,"
				+ "			 PIB_ALL.PH_WORKTYPE,"
				+ "			 PIB_ALL.INS_RELATION,"
				+ "			 PIB_ALL.INS_NAME,"
				+ "			 PIB_ALL.INS_BIRTHDAY,"
				+ "			 PIB_ALL.BENE_RELATION,"
				+ "			 PIB_ALL.BENE_NAME,"
				+ "			 PIB_ALL.BENE_BIRTHDAY,"
				+ "			 PIB_ALL.SUCCESS,"
				+ "			 PIB_ALL.FORM_ID FROM ("
				+ "	  SELECT *"
				+ "	   FROM  Tmp_srcData"
				+ "	  WHERE  EVENT = '个人信息'"
				+ "		AND  SUBTYPE = '下一步') PIB_ALL"
				+ "	INNER JOIN ("
				+ "	  SELECT CONCAT_WS(',',COLLECT_SET(TERMINAL_ID)) AS TERMINAL_ID,"
				+ "			 USER_ID,"
				+ "			 LRT_ID,"
				+ "			 FORM_ID,"
				+ "			 EVENT,"
				+ "			 MAX(CAST(VISIT_TIME AS BIGINT)) AS VISIT_TIME"
				+ "		FROM Tmp_srcData"
				+ "	   WHERE EVENT = '个人信息'"
				+ "		 AND SUBTYPE = '下一步'"
				+ "	   GROUP BY "
				+ "				USER_ID,"
				+ "				LRT_ID,"
				+ "				FORM_ID,"
				+ "				EVENT"
				+ "		) PIB_MAX"
				+ "	 ON  	spequals(PIB_ALL.TERMINAL_ID,PIB_MAX.TERMINAL_ID,',') = '1'"
				+ "	   AND  PIB_ALL.USER_ID = PIB_MAX.USER_ID"
				+ "	   AND  PIB_ALL.LRT_ID = PIB_MAX.LRT_ID"
				+ "	   AND  PIB_ALL.FORM_ID = PIB_MAX.FORM_ID"
				+ "	   AND  PIB_ALL.VISIT_TIME = PIB_MAX.VISIT_TIME";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_personInfoBtn",DataFrameUtil.CACHETABLE_PARQUET);
	}

	/**
	 * 
	 * @Description: 获取个人信息页面数据
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月24日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getPersonInfoPage(HiveContext sqlContext) {
		String sql = "SELECT PIP_ALL.APP_TYPE,"
				+ "			 PIP_ALL.APP_ID,"
				+ "			 PIP_ALL.EVENT,"
				+ "			 PIP_ALL.SUBTYPE,"
				+ "			 PIP_MAX.TERMINAL_ID,"
				+ "			 PIP_ALL.USER_ID,"
				+ "			 PIP_ALL.VISIT_TIME,"
				+ "			 PIP_ALL.FROM_ID,"
				+ "			 PIP_ALL.LRT_ID,"
				+ "			 PIP_ALL.SALE_MODE,"
				+ "			 PIP_ALL.BABY_GENDER,"
				+ "			 PIP_ALL.BABY_BIRTHDAY,"
				+ "			 PIP_ALL.INSURE_TYPE,"
				+ "			 PIP_ALL.SEX,"
				+ "			 PIP_ALL.BABY_RELATION,"
				+ "			 PIP_ALL.P_BIRTHDAY,"
				+ "			 PIP_ALL.PAY_TYPE,"
				+ "			 PIP_ALL.PAY_PERIOD,"
				+ "			 PIP_ALL.PH_NAME,"
				+ "			 PIP_ALL.PH_BIRTHDAY,"
				+ "			 PIP_ALL.PH_EMAIL,"
				+ "			 PIP_ALL.PH_INDUSTRY,"
				+ "			 PIP_ALL.PH_WORKTYPE,"
				+ "			 PIP_ALL.INS_RELATION,"
				+ "			 PIP_ALL.INS_NAME,"
				+ "			 PIP_ALL.INS_BIRTHDAY,"
				+ "			 PIP_ALL.BENE_RELATION,"
				+ "			 PIP_ALL.BENE_NAME,"
				+ "			 PIP_ALL.BENE_BIRTHDAY,"
				+ "			 PIP_ALL.SUCCESS,"
				+ "			 PIP_ALL.FORM_ID FROM ("
				+ "	  SELECT *"
				+ "	   FROM  Tmp_srcData"
				+ "	  WHERE  EVENT = '个人信息'"
				+ "		AND  (SUBTYPE = '' OR SUBTYPE IS NULL)) PIP_ALL"
				+ "	INNER JOIN ("
				+ "	  SELECT CONCAT_WS(',',COLLECT_SET(TERMINAL_ID)) AS TERMINAL_ID,"
				+ "			 USER_ID,"
				+ "			 LRT_ID,"
				+ "			 FORM_ID,"
				+ "			 EVENT,"
				+ "			 MAX(CAST(VISIT_TIME AS BIGINT)) AS VISIT_TIME"
				+ "		FROM Tmp_srcData"
				+ "	   WHERE EVENT = '个人信息' "
				+ "		AND  (SUBTYPE = '' OR SUBTYPE IS NULL)"
				+ "	   GROUP BY "
				+ "				USER_ID,"
				+ "				LRT_ID,"
				+ "				FORM_ID,"
				+ "				EVENT"
				+ "		) PIP_MAX"
				+ "	 ON  	spequals(PIP_ALL.TERMINAL_ID,PIP_MAX.TERMINAL_ID,',') = '1'"
				+ "	   AND  PIP_ALL.USER_ID = PIP_MAX.USER_ID"
				+ "	   AND  PIP_ALL.LRT_ID = PIP_MAX.LRT_ID"
				+ "	   AND  PIP_ALL.FORM_ID = PIP_MAX.FORM_ID"
				+ "	   AND  PIP_ALL.VISIT_TIME = PIP_MAX.VISIT_TIME";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_personInfoPage",DataFrameUtil.CACHETABLE_PARQUET);
	}

	/**
	 * 
	 * @Description: 获取支付返回页面数据
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月23日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getPaymentBackPage(HiveContext sqlContext) {
		String sql = "SELECT PMBP_ALL.APP_TYPE,"
				+ "			 PMBP_ALL.APP_ID,"
				+ "			 PMBP_ALL.EVENT,"
				+ "			 PMBP_ALL.SUBTYPE,"
				+ "			 PMBP_MAX.TERMINAL_ID,"
				+ "			 PMBP_ALL.USER_ID,"
				+ "			 PMBP_ALL.VISIT_TIME,"
				+ "			 PMBP_ALL.FROM_ID,"
				+ "			 PMBP_ALL.LRT_ID,"
				+ "			 PMBP_ALL.SALE_MODE,"
				+ "			 PMBP_ALL.BABY_GENDER,"
				+ "			 PMBP_ALL.BABY_BIRTHDAY,"
				+ "			 PMBP_ALL.INSURE_TYPE,"
				+ "			 PMBP_ALL.SEX,"
				+ "			 PMBP_ALL.BABY_RELATION,"
				+ "			 PMBP_ALL.P_BIRTHDAY,"
				+ "			 PMBP_ALL.PAY_TYPE,"
				+ "			 PMBP_ALL.PAY_PERIOD,"
				+ "			 PMBP_ALL.PH_NAME,"
				+ "			 PMBP_ALL.PH_BIRTHDAY,"
				+ "			 PMBP_ALL.PH_EMAIL,"
				+ "			 PMBP_ALL.PH_INDUSTRY,"
				+ "			 PMBP_ALL.PH_WORKTYPE,"
				+ "			 PMBP_ALL.INS_RELATION,"
				+ "			 PMBP_ALL.INS_NAME,"
				+ "			 PMBP_ALL.INS_BIRTHDAY,"
				+ "			 PMBP_ALL.BENE_RELATION,"
				+ "			 PMBP_ALL.BENE_NAME,"
				+ "			 PMBP_ALL.BENE_BIRTHDAY,"
				+ "			 PMBP_ALL.SUCCESS,"
				+ "			 PMBP_ALL.FORM_ID FROM ("
				+ "	  SELECT *"
				+ "	   FROM  Tmp_srcData"
				+ "	  WHERE  EVENT = '支付返回'"
				+ "		AND  (SUBTYPE = '' OR SUBTYPE IS NULL)) PMBP_ALL"
				+ "	INNER JOIN ("
				+ "	  SELECT CONCAT_WS(',',COLLECT_SET(TERMINAL_ID)) AS TERMINAL_ID,"
				+ "			 USER_ID,"
				+ "			 LRT_ID,"
				+ "			 FORM_ID,"
				+ "			 EVENT,"
				+ "			 MAX(CAST(VISIT_TIME AS BIGINT)) AS VISIT_TIME"
				+ "		FROM Tmp_srcData"
				+ "	   WHERE EVENT = '支付返回' "
				+ "		AND  (SUBTYPE = '' OR SUBTYPE IS NULL)"
				+ "	   GROUP BY "
				+ "				USER_ID,"
				+ "				LRT_ID,"
				+ "				FORM_ID,"
				+ "				EVENT"
				+ "		) PMBP_MAX"
				+ "	 ON  	spequals(PMBP_ALL.TERMINAL_ID,PMBP_MAX.TERMINAL_ID,',') = '1'"
				+ "	   AND  PMBP_ALL.USER_ID = PMBP_MAX.USER_ID"
				+ "	   AND  PMBP_ALL.LRT_ID = PMBP_MAX.LRT_ID"
				+ "	   AND  PMBP_ALL.FORM_ID = PMBP_MAX.FORM_ID"
				+ "	   AND  PMBP_ALL.VISIT_TIME = PMBP_MAX.VISIT_TIME";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_paymentBackPage",DataFrameUtil.CACHETABLE_PARQUET);
	}

	/**
	 * 
	 * @Description: 获取支付页面点击下一步按钮数据
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月23日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getPaymentBtn(HiveContext sqlContext) {
		String sql = "SELECT PMB_ALL.APP_TYPE,"
				+ "			 PMB_ALL.APP_ID,"
				+ "			 PMB_ALL.EVENT,"
				+ "			 PMB_ALL.SUBTYPE,"
				+ "			 PMB_MAX.TERMINAL_ID,"
				+ "			 PMB_ALL.USER_ID,"
				+ "			 PMB_ALL.VISIT_TIME,"
				+ "			 PMB_ALL.FROM_ID,"
				+ "			 PMB_ALL.LRT_ID,"
				+ "			 PMB_ALL.SALE_MODE,"
				+ "			 PMB_ALL.BABY_GENDER,"
				+ "			 PMB_ALL.BABY_BIRTHDAY,"
				+ "			 PMB_ALL.INSURE_TYPE,"
				+ "			 PMB_ALL.SEX,"
				+ "			 PMB_ALL.BABY_RELATION,"
				+ "			 PMB_ALL.P_BIRTHDAY,"
				+ "			 PMB_ALL.PAY_TYPE,"
				+ "			 PMB_ALL.PAY_PERIOD,"
				+ "			 PMB_ALL.PH_NAME,"
				+ "			 PMB_ALL.PH_BIRTHDAY,"
				+ "			 PMB_ALL.PH_EMAIL,"
				+ "			 PMB_ALL.PH_INDUSTRY,"
				+ "			 PMB_ALL.PH_WORKTYPE,"
				+ "			 PMB_ALL.INS_RELATION,"
				+ "			 PMB_ALL.INS_NAME,"
				+ "			 PMB_ALL.INS_BIRTHDAY,"
				+ "			 PMB_ALL.BENE_RELATION,"
				+ "			 PMB_ALL.BENE_NAME,"
				+ "			 PMB_ALL.BENE_BIRTHDAY,"
				+ "			 PMB_ALL.SUCCESS,"
				+ "			 PMB_ALL.FORM_ID FROM ("
				+ "	  SELECT *"
				+ "	   FROM  Tmp_srcData"
				+ "	  WHERE  EVENT = '支付页面'"
				+ "		AND  SUBTYPE = '下一步') PMB_ALL"
				+ "	INNER JOIN ("
				+ "	  SELECT CONCAT_WS(',',COLLECT_SET(TERMINAL_ID)) AS TERMINAL_ID,"
				+ "			 USER_ID,"
				+ "			 LRT_ID,"
				+ "			 EVENT,"
				+ "			 FORM_ID,"
				+ "			 MAX(CAST(VISIT_TIME AS BIGINT)) AS VISIT_TIME"
				+ "		FROM Tmp_srcData"
				+ "	   WHERE EVENT = '支付页面'"
				+ "		 AND SUBTYPE = '下一步'"
				+ "	   GROUP BY "
				+ "				USER_ID,"
				+ "				LRT_ID,"
				+ "				FORM_ID,"
				+ "				EVENT"
				+ "		) PMB_MAX"
				+ "	 ON  	spequals(PMB_ALL.TERMINAL_ID,PMB_MAX.TERMINAL_ID,',') = '1'"
				+ "	   AND  PMB_ALL.USER_ID = PMB_MAX.USER_ID"
				+ "	   AND  PMB_ALL.LRT_ID = PMB_MAX.LRT_ID"
				+ "	   AND  PMB_ALL.FORM_ID = PMB_MAX.FORM_ID"
				+ "	   AND  PMB_ALL.VISIT_TIME = PMB_MAX.VISIT_TIME";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_paymentBtn",DataFrameUtil.CACHETABLE_PARQUET);
	}

	/**
	 * 
	 * @Description: 获取支付页面数据
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月23日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getPaymentPage(HiveContext sqlContext) {
		String sql = "SELECT PMP_ALL.APP_TYPE,"
				+ "			 PMP_ALL.APP_ID,"
				+ "			 PMP_ALL.EVENT,"
				+ "			 PMP_ALL.SUBTYPE,"
				+ "			 PMP_MAX.TERMINAL_ID,"
				+ "			 PMP_ALL.USER_ID,"
				+ "			 PMP_ALL.VISIT_TIME,"
				+ "			 PMP_ALL.FROM_ID,"
				+ "			 PMP_ALL.LRT_ID,"
				+ "			 PMP_ALL.SALE_MODE,"
				+ "			 PMP_ALL.BABY_GENDER,"
				+ "			 PMP_ALL.BABY_BIRTHDAY,"
				+ "			 PMP_ALL.INSURE_TYPE,"
				+ "			 PMP_ALL.SEX,"
				+ "			 PMP_ALL.BABY_RELATION,"
				+ "			 PMP_ALL.P_BIRTHDAY,"
				+ "			 PMP_ALL.PAY_TYPE,"
				+ "			 PMP_ALL.PAY_PERIOD,"
				+ "			 PMP_ALL.PH_NAME,"
				+ "			 PMP_ALL.PH_BIRTHDAY,"
				+ "			 PMP_ALL.PH_EMAIL,"
				+ "			 PMP_ALL.PH_INDUSTRY,"
				+ "			 PMP_ALL.PH_WORKTYPE,"
				+ "			 PMP_ALL.INS_RELATION,"
				+ "			 PMP_ALL.INS_NAME,"
				+ "			 PMP_ALL.INS_BIRTHDAY,"
				+ "			 PMP_ALL.BENE_RELATION,"
				+ "			 PMP_ALL.BENE_NAME,"
				+ "			 PMP_ALL.BENE_BIRTHDAY,"
				+ "			 PMP_ALL.SUCCESS,"
				+ "			 PMP_ALL.FORM_ID FROM ("
				+ "	  SELECT *"
				+ "	   FROM  Tmp_srcData"
				+ "	  WHERE  EVENT = '支付页面'"
				+ "		AND  (SUBTYPE = '' OR SUBTYPE IS NULL)) PMP_ALL"
				+ "	INNER JOIN ("
				+ "	  SELECT CONCAT_WS(',',COLLECT_SET(TERMINAL_ID)) AS TERMINAL_ID,"
				+ "			 USER_ID,"
				+ "			 LRT_ID,"
				+ "			 FORM_ID,"
				+ "			 EVENT,"
				+ "			 MAX(CAST(VISIT_TIME AS BIGINT)) AS VISIT_TIME"
				+ "		FROM Tmp_srcData"
				+ "	   WHERE EVENT = '支付页面' "
				+ "		AND  (SUBTYPE = '' OR SUBTYPE IS NULL)"
				+ "	   GROUP BY "
				+ "				USER_ID,"
				+ "				LRT_ID,"
				+ "				FORM_ID,"
				+ "				EVENT"
				+ "		) PMP_MAX"
				+ "	 ON  	spequals(PMP_ALL.TERMINAL_ID,PMP_MAX.TERMINAL_ID,',') = '1'"
				+ "	   AND  PMP_ALL.USER_ID = PMP_MAX.USER_ID"
				+ "	   AND  PMP_ALL.LRT_ID = PMP_MAX.LRT_ID"
				+ "	   AND  PMP_ALL.FORM_ID = PMP_MAX.FORM_ID"
				+ "	   AND  PMP_ALL.VISIT_TIME = PMP_MAX.VISIT_TIME";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_paymentPage",DataFrameUtil.CACHETABLE_PARQUET);
	}

	/**
	 * 
	 * @Description: 获取健康告知点击下一步按钮数据
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月23日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getHealthInformBtn(HiveContext sqlContext) {
		String sql = "SELECT HIB_ALL.APP_TYPE,"
				+ "			 HIB_ALL.APP_ID,"
				+ "			 HIB_ALL.EVENT,"
				+ "			 HIB_ALL.SUBTYPE,"
				+ "			 HIB_MAX.TERMINAL_ID,"
				+ "			 HIB_ALL.USER_ID,"
				+ "			 HIB_ALL.VISIT_TIME,"
				+ "			 HIB_ALL.FROM_ID,"
				+ "			 HIB_ALL.LRT_ID,"
				+ "			 HIB_ALL.SALE_MODE,"
				+ "			 HIB_ALL.BABY_GENDER,"
				+ "			 HIB_ALL.BABY_BIRTHDAY,"
				+ "			 HIB_ALL.INSURE_TYPE,"
				+ "			 HIB_ALL.SEX,"
				+ "			 HIB_ALL.BABY_RELATION,"
				+ "			 HIB_ALL.P_BIRTHDAY,"
				+ "			 HIB_ALL.PAY_TYPE,"
				+ "			 HIB_ALL.PAY_PERIOD,"
				+ "			 HIB_ALL.PH_NAME,"
				+ "			 HIB_ALL.PH_BIRTHDAY,"
				+ "			 HIB_ALL.PH_EMAIL,"
				+ "			 HIB_ALL.PH_INDUSTRY,"
				+ "			 HIB_ALL.PH_WORKTYPE,"
				+ "			 HIB_ALL.INS_RELATION,"
				+ "			 HIB_ALL.INS_NAME,"
				+ "			 HIB_ALL.INS_BIRTHDAY,"
				+ "			 HIB_ALL.BENE_RELATION,"
				+ "			 HIB_ALL.BENE_NAME,"
				+ "			 HIB_ALL.BENE_BIRTHDAY,"
				+ "			 HIB_ALL.SUCCESS,"
				+ "			 HIB_ALL.FORM_ID FROM ("
				+ "	  SELECT *"
				+ "	   FROM  Tmp_srcData"
				+ "	  WHERE  EVENT = '健康告知'"
				+ "		AND  SUBTYPE = '下一步') HIB_ALL"
				+ "	INNER JOIN ("
				+ "	  SELECT CONCAT_WS(',',COLLECT_SET(TERMINAL_ID)) AS TERMINAL_ID,"
				+ "			 USER_ID,"
				+ "			 LRT_ID,"
				+ "			 FORM_ID,"
				+ "			 EVENT,"
				+ "			 MAX(CAST(VISIT_TIME AS BIGINT)) AS VISIT_TIME"
				+ "		FROM Tmp_srcData"
				+ "	   WHERE EVENT = '健康告知'"
				+ "		 AND SUBTYPE = '下一步'"
				+ "	   GROUP BY "
				+ "				USER_ID,"
				+ "				LRT_ID,"
				+ "				FORM_ID,"
				+ "				EVENT"
				+ "		) HIB_MAX"
				+ "	 ON  	spequals(HIB_ALL.TERMINAL_ID,HIB_MAX.TERMINAL_ID,',') = '1'"
				+ "	   AND  HIB_ALL.USER_ID = HIB_MAX.USER_ID"
				+ "	   AND  HIB_ALL.LRT_ID = HIB_MAX.LRT_ID"
				+ "	   AND  HIB_ALL.FORM_ID = HIB_MAX.FORM_ID"
				+ "	   AND  HIB_ALL.VISIT_TIME = HIB_MAX.VISIT_TIME";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_healthInformBtn",DataFrameUtil.CACHETABLE_PARQUET);
	}

	/**
	 * 
	 * @Description: 获取健康告知页面数据
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月23日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getHealthInformPage(HiveContext sqlContext) {
		String sql = "SELECT HIP_ALL.APP_TYPE,"
				+ "			 HIP_ALL.APP_ID,"
				+ "			 HIP_ALL.EVENT,"
				+ "			 HIP_ALL.SUBTYPE,"
				+ "			 HIP_MAX.TERMINAL_ID,"
				+ "			 HIP_ALL.USER_ID,"
				+ "			 HIP_ALL.VISIT_TIME,"
				+ "			 HIP_ALL.FROM_ID,"
				+ "			 HIP_ALL.LRT_ID,"
				+ "			 HIP_ALL.SALE_MODE,"
				+ "			 HIP_ALL.BABY_GENDER,"
				+ "			 HIP_ALL.BABY_BIRTHDAY,"
				+ "			 HIP_ALL.INSURE_TYPE,"
				+ "			 HIP_ALL.SEX,"
				+ "			 HIP_ALL.BABY_RELATION,"
				+ "			 HIP_ALL.P_BIRTHDAY,"
				+ "			 HIP_ALL.PAY_TYPE,"
				+ "			 HIP_ALL.PAY_PERIOD,"
				+ "			 HIP_ALL.PH_NAME,"
				+ "			 HIP_ALL.PH_BIRTHDAY,"
				+ "			 HIP_ALL.PH_EMAIL,"
				+ "			 HIP_ALL.PH_INDUSTRY,"
				+ "			 HIP_ALL.PH_WORKTYPE,"
				+ "			 HIP_ALL.INS_RELATION,"
				+ "			 HIP_ALL.INS_NAME,"
				+ "			 HIP_ALL.INS_BIRTHDAY,"
				+ "			 HIP_ALL.BENE_RELATION,"
				+ "			 HIP_ALL.BENE_NAME,"
				+ "			 HIP_ALL.BENE_BIRTHDAY,"
				+ "			 HIP_ALL.SUCCESS,"
				+ "			 HIP_ALL.FORM_ID FROM ("
				+ "	  SELECT *"
				+ "	   FROM  Tmp_srcData"
				+ "	  WHERE  EVENT = '健康告知'"
				+ "		AND  (SUBTYPE = '' OR SUBTYPE IS NULL)) HIP_ALL"
				+ "	INNER JOIN ("
				+ "	  SELECT CONCAT_WS(',',COLLECT_SET(TERMINAL_ID)) AS TERMINAL_ID,"
				+ "			 USER_ID,"
				+ "			 LRT_ID,"
				+ "			 FORM_ID,"
				+ "			 EVENT,"
				+ "			 MAX(CAST(VISIT_TIME AS BIGINT)) AS VISIT_TIME"
				+ "		FROM Tmp_srcData"
				+ "	   WHERE EVENT = '健康告知' "
				+ "		AND  (SUBTYPE = '' OR SUBTYPE IS NULL)"
				+ "	   GROUP BY "
				+ "				USER_ID,"
				+ "				LRT_ID,"
				+ "				FORM_ID,"
				+ "				EVENT"
				+ "		) HIP_MAX"
				+ "	 ON  	spequals(HIP_ALL.TERMINAL_ID,HIP_MAX.TERMINAL_ID,',') = '1'"
				+ "	   AND  HIP_ALL.USER_ID = HIP_MAX.USER_ID"
				+ "	   AND  HIP_ALL.LRT_ID = HIP_MAX.LRT_ID"
				+ "	   AND  HIP_ALL.FORM_ID = HIP_MAX.FORM_ID"
				+ "	   AND  HIP_ALL.VISIT_TIME = HIP_MAX.VISIT_TIME";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_healthInformPage",DataFrameUtil.CACHETABLE_PARQUET);
		
	}

	/**
	 * 
	 * @Description: 获取点击立即购买按钮数据
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月23日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getPremiumTestBtn(HiveContext sqlContext) {
		String sql = "SELECT TESTBTN_ALL.APP_TYPE,"
				+ "			 TESTBTN_ALL.APP_ID,"
				+ "			 TESTBTN_ALL.EVENT,"
				+ "			 TESTBTN_ALL.SUBTYPE,"
				+ "			 TESTBTN_MAX.TERMINAL_ID,"
				+ "			 TESTBTN_ALL.USER_ID,"
				+ "			 TESTBTN_ALL.VISIT_TIME,"
				+ "			 TESTBTN_ALL.FROM_ID,"
				+ "			 TESTBTN_ALL.LRT_ID,"
				+ "			 TESTBTN_ALL.SALE_MODE,"
				+ "			 TESTBTN_ALL.BABY_GENDER,"
				+ "			 TESTBTN_ALL.BABY_BIRTHDAY,"
				+ "			 TESTBTN_ALL.INSURE_TYPE,"
				+ "			 TESTBTN_ALL.SEX,"
				+ "			 TESTBTN_ALL.BABY_RELATION,"
				+ "			 TESTBTN_ALL.P_BIRTHDAY,"
				+ "			 TESTBTN_ALL.PAY_TYPE,"
				+ "			 TESTBTN_ALL.PAY_PERIOD,"
				+ "			 TESTBTN_ALL.PH_NAME,"
				+ "			 TESTBTN_ALL.PH_BIRTHDAY,"
				+ "			 TESTBTN_ALL.PH_EMAIL,"
				+ "			 TESTBTN_ALL.PH_INDUSTRY,"
				+ "			 TESTBTN_ALL.PH_WORKTYPE,"
				+ "			 TESTBTN_ALL.INS_RELATION,"
				+ "			 TESTBTN_ALL.INS_NAME,"
				+ "			 TESTBTN_ALL.INS_BIRTHDAY,"
				+ "			 TESTBTN_ALL.BENE_RELATION,"
				+ "			 TESTBTN_ALL.BENE_NAME,"
				+ "			 TESTBTN_ALL.BENE_BIRTHDAY,"
				+ "			 TESTBTN_ALL.SUCCESS,"
				+ "			 TESTBTN_ALL.FORM_ID FROM ("
				+ "	  SELECT *"
				+ "	   FROM  Tmp_srcData"
				+ "	  WHERE  EVENT = '保费测算'"
				+ "		AND  SUBTYPE = '立即购买') TESTBTN_ALL"
				+ "	INNER JOIN ("
				+ "	  SELECT CONCAT_WS(',',COLLECT_SET(TSD.TERMINAL_ID)) AS TERMINAL_ID,"
				+ "			 TSD.DAY,"
				+ "			 TSD.USER_ID,"
				+ "			 TSD.LRT_ID,"
				+ "			 TSD.FORM_ID,"
				+ "			 TSD.EVENT,"
				+ "			 MAX(CAST(TSD.VISIT_TIME AS BIGINT)) AS VISIT_TIME"
				+ "		FROM (SELECT TERMINAL_ID,"
				+ "					 FROM_UNIXTIME(CAST(VISIT_TIME/1000 AS BIGINT),'yyyy-MM-dd') AS DAY,"
				+ "					 USER_ID,"
				+ "					 LRT_ID,"
				+ "					 FORM_ID,"
				+ "					 EVENT,"
				+ "					 VISIT_TIME"
				+ "				FROM Tmp_srcData"
				+ "			   WHERE EVENT = '保费测算'"
				+ "		 		 AND SUBTYPE = '立即购买') TSD"
				+ "	   GROUP BY TSD.DAY,"
				+ "				TSD.USER_ID,"
				+ "				TSD.LRT_ID,"
				+ "				TSD.FORM_ID,"
				+ "				TSD.EVENT"
				+ "		) TESTBTN_MAX"
				+ "	 ON  	spequals(TESTBTN_ALL.TERMINAL_ID,TESTBTN_MAX.TERMINAL_ID,',') = '1'"
				+ "	   AND  TESTBTN_ALL.USER_ID = TESTBTN_MAX.USER_ID"
				+ "	   AND  TESTBTN_ALL.LRT_ID = TESTBTN_MAX.LRT_ID"
				+ "	   AND  TESTBTN_ALL.FORM_ID = TESTBTN_MAX.FORM_ID"
				+ "	   AND  TESTBTN_ALL.VISIT_TIME = TESTBTN_MAX.VISIT_TIME";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_premiumTestBtn",DataFrameUtil.CACHETABLE_PARQUET);
	}

	/**
	 * 
	 * @Description: 获取保费测算页面数据(取最初的一条)
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月23日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getPremiumTestPage(HiveContext sqlContext) {
		String sql = "SELECT TESTPG_ALL.APP_TYPE,"
				+ "			 TESTPG_ALL.APP_ID,"
				+ "			 TESTPG_ALL.EVENT,"
				+ "			 TESTPG_ALL.SUBTYPE,"
				+ "			 TESTPG_MIN.TERMINAL_ID,"
				+ "			 TESTPG_ALL.USER_ID,"
				+ "			 TESTPG_ALL.VISIT_TIME,"
				+ "			 TESTPG_ALL.FROM_ID,"
				+ "			 TESTPG_ALL.LRT_ID,"
				+ "			 TESTPG_ALL.SALE_MODE,"
				+ "			 TESTPG_ALL.BABY_GENDER,"
				+ "			 TESTPG_ALL.BABY_BIRTHDAY,"
				+ "			 TESTPG_ALL.INSURE_TYPE,"
				+ "			 TESTPG_ALL.SEX,"
				+ "			 TESTPG_ALL.BABY_RELATION,"
				+ "			 TESTPG_ALL.P_BIRTHDAY,"
				+ "			 TESTPG_ALL.PAY_TYPE,"
				+ "			 TESTPG_ALL.PAY_PERIOD,"
				+ "			 TESTPG_ALL.PH_NAME,"
				+ "			 TESTPG_ALL.PH_BIRTHDAY,"
				+ "			 TESTPG_ALL.PH_EMAIL,"
				+ "			 TESTPG_ALL.PH_INDUSTRY,"
				+ "			 TESTPG_ALL.PH_WORKTYPE,"
				+ "			 TESTPG_ALL.INS_RELATION,"
				+ "			 TESTPG_ALL.INS_NAME,"
				+ "			 TESTPG_ALL.INS_BIRTHDAY,"
				+ "			 TESTPG_ALL.BENE_RELATION,"
				+ "			 TESTPG_ALL.BENE_NAME,"
				+ "			 TESTPG_ALL.BENE_BIRTHDAY,"
				+ "			 TESTPG_ALL.SUCCESS,"
				+ "			 TESTPG_ALL.FORM_ID"
				+ "	 FROM ("
				+ "	  SELECT *"
				+ "	   FROM  Tmp_srcData"
				+ "	  WHERE  EVENT = '保费测算'"
				+ "		AND  (SUBTYPE = '' OR SUBTYPE IS NULL)) TESTPG_ALL"
				+ "	INNER JOIN ("
				+ "	  SELECT CONCAT_WS(',',COLLECT_SET(TSD.TERMINAL_ID)) AS TERMINAL_ID,"
				+ "			 TSD.DAY,"
				+ "			 TSD.USER_ID,"
				+ "			 TSD.LRT_ID,"
				+ "			 TSD.FORM_ID,"
				+ "			 TSD.EVENT,"
				+ "			 MIN(CAST(TSD.VISIT_TIME AS BIGINT)) AS VISIT_TIME"
				+ "		FROM (SELECT TERMINAL_ID,"
				+ "					 FROM_UNIXTIME(CAST(VISIT_TIME/1000 AS BIGINT),'yyyy-MM-dd') AS DAY,"
				+ "					 USER_ID,"
				+ "					 LRT_ID,"
				+ "					 FORM_ID,"
				+ "					 EVENT,"
				+ "					 VISIT_TIME"
				+ "				FROM Tmp_srcData"
				+ "			   WHERE EVENT = '保费测算'"
				+ "		 		AND  (SUBTYPE = '' OR SUBTYPE IS NULL)) TSD"
				+ "	   GROUP BY TSD.DAY,"
				+ "				TSD.USER_ID,"
				+ "				TSD.LRT_ID,"
				+ "				TSD.FORM_ID,"
				+ "				TSD.EVENT"
				+ "		) TESTPG_MIN"
				+ "	 ON  	spequals(TESTPG_ALL.TERMINAL_ID,TESTPG_MIN.TERMINAL_ID,',') = '1'"
				+ "	   AND  TESTPG_ALL.USER_ID = TESTPG_MIN.USER_ID"
				+ "	   AND  TESTPG_ALL.LRT_ID = TESTPG_MIN.LRT_ID"
				+ "	   AND  TESTPG_ALL.FORM_ID = TESTPG_MIN.FORM_ID"
				+ "	   AND  TESTPG_ALL.VISIT_TIME = TESTPG_MIN.VISIT_TIME";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_premiumTestPage",DataFrameUtil.CACHETABLE_PARQUET);
	}

	

	/**
	 * 
	 * @Description: 解析label和customize
	 * @param df
	 * @return
	 * @author moyunqing
	 * @date 2016年9月23日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private JavaRDD<InsuranceProcessTmp> analysisFields(DataFrame df) {
		
		return df.javaRDD().map(new Function<Row, InsuranceProcessTmp>() {
			
			private static final long serialVersionUID = -516113301879207296L;
			
			public InsuranceProcessTmp call(Row v1) throws Exception {
				InsuranceProcessTmp ipt = new InsuranceProcessTmp();
				ipt.setAPP_TYPE(v1.getString(0));
				ipt.setAPP_ID(v1.getString(1));
				ipt.setEVENT(v1.getString(2));
				ipt.setSUBTYPE(v1.getString(3));
				//去除terminal_id中特殊的字符
				ipt.setTERMINAL_ID(v1.getString(4).replaceAll("\\.|\\t|\\r|\\n|\\s|\"|\'", ""));
				ipt.setUSER_ID(v1.getString(5));
				ipt.setVISIT_TIME(v1.getString(8));
				ipt.setFROM_ID(v1.getString(9));
				StringParse parse = new StringParse();
				parse.appendSpecialValueRegex("x22|-22|X22");
				Map<String,String> dataMap = parse.parse2Map(v1.getString(6));//解析label
				ipt.setLRT_ID(dataMap.get("lrtId"));
				ipt.setLRT_NAME(dataMap.get("lrtName"));
				ipt.setFORM_ID(dataMap.get("formId"));
				ipt.setSALE_MODE(dataMap.get("saleMode"));
				dataMap = parse.parse2Map(v1.getString(7));//解析customize
				ipt.setBABY_GENDER(dataMap.get("babyGender"));
				ipt.setBABY_BIRTHDAY(dataMap.get("babyBirthday"));
				ipt.setINSURE_TYPE(dataMap.get("insureType"));
				ipt.setSEX(dataMap.get("sex"));
				ipt.setBABY_RELATION(dataMap.get("relation"));
				ipt.setP_BIRTHDAY(dataMap.get("pBirthday"));
				ipt.setPAY_TYPE(dataMap.get("payType"));
				ipt.setPAY_PERIOD(dataMap.get("payPeriod"));
				ipt.setPH_NAME(dataMap.get("ph_name"));
				ipt.setPH_BIRTHDAY(dataMap.get("ph_birthday"));
				ipt.setPH_EMAIL(dataMap.get("ph_email"));
				ipt.setPH_INDUSTRY(dataMap.get("ph_industry"));
				ipt.setPH_WORKTYPE(dataMap.get("ph_worktype"));
				ipt.setINS_BIRTHDAY(dataMap.get("ins_birthday"));
				ipt.setINS_NAME(dataMap.get("ins_name"));
				ipt.setINS_RELATION(dataMap.get("ins_relation"));
				ipt.setBENE_NAME(dataMap.get(("bene_name")));
				ipt.setBENE_BIRTHDAY(dataMap.get("bene_birthday"));
				ipt.setBENE_RELATION(dataMap.get("bene_relation"));
				ipt.setSUCCESS(dataMap.get("success"));
				return ipt;
			}
		});
	}
	
	/**
	 * 
	 * @Description: 加载原始日志表数据
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月22日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getSrcData(HiveContext sqlContext,boolean isall) {
		String sql = "SELECT APP_TYPE,"
				+ "			 APP_ID,"
				+ "			 EVENT,"
				+ "			 SUBTYPE,"
				+ "			 TERMINAL_ID,"
				+ "			 USER_ID,"
				+ "			 LABEL,"
				+ "			 CUSTOM_VAL,"
				+ "			 CASE WHEN CLIENTTIME = '' OR CLIENTTIME IS NULL THEN TIME"
				+ "				  ELSE CLIENTTIME"
				+ "			 END  AS VISIT_TIME,"
				+ "			 FROM_ID"
				+ "	   FROM  Tmp_uba"
				+ "	  WHERE  LOWER(APP_TYPE) = 'javaweb'"
				+ "		 AND LOWER(APP_ID) = 'javaweb001'"
				+ "		 AND LOWER(EVENT) <> 'page.load'"
				+ "		 AND LOWER(EVENT) <> 'page.unload'"
				+ "	     AND EVENT <> ''"
				+ "		 AND EVENT IS NOT NULL";
		if(!isall)
			sql += "	AND FROM_UNIXTIME(CAST(CAST(TIME AS BIGINT)/1000 AS BIGINT),'yyyy-MM-dd') = '" + getDate(-1) + "'";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_src",DataFrameUtil.CACHETABLE_PARQUET);
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
