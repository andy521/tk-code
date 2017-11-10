package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.fact.sparksql.desttable.FactChildrenCare;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;


public class ChildrenCare implements Serializable {
	
	private static final long serialVersionUID = 1L;
	String sca_ol = TK_DataFormatConvertUtil.getSchema();
	String date = "";

	public DataFrame getChildrenCareDF(HiveContext sqlContext,String appids) {

		getGWOrg(sqlContext);
		getInsurantTmp(sqlContext);
		getInsurantDetailTmp(sqlContext);
		getInsureAllTMP(sqlContext);
		getGatherInsurantTmp(sqlContext);
		sqlContext.createDataFrame(analysisFields(getCCtaticEventTmp(sqlContext, appids)), FactChildrenCare.class).registerTempTable("TMP_CC");
		getChildrenCareClueTmp(sqlContext);
		getChildrenCareClue(sqlContext);
		return getChildrenCareClueResult(sqlContext);

	}
	
	

	public DataFrame getChildrenCareOthersDF(HiveContext sqlContext,String appids,String year) {
		getOtherInsurantTmp(sqlContext);
		getOtherInsurantDetailTmp(sqlContext);
		getOtherInsureAllTMP(sqlContext);
		getThresholdTmp(sqlContext,year);
		getGatherOtherInsurantTmp(sqlContext);
		getOtherInsurantResult(sqlContext);
		sqlContext.createDataFrame(analysisFields(getCCtaticEventTmp(sqlContext, appids)), FactChildrenCare.class).registerTempTable("TMP_CC");
		getOtherChildrenCareClueTmp(sqlContext);
		getOtherChildrenCareClue(sqlContext);
		return getOtherChildrenCareClueResult(sqlContext);
		
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



	/**
	 * 只取官网渠道的，渠道合作会引起合作
	 * @Description: 
	 * @param sqlContext
	 * @return
	 * @author itw_qiuzq01
	 * @date 2017年3月10日
	 * @update [日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getGWOrg(HiveContext sqlContext) {
		String hql = "SELECT DISTINCT ORGANIZATION_ID"
				+"  FROM TKOLDB.P_CLIENTORG"
				+" WHERE (ORGANIZATION_CODE LIKE '001011091%' OR"
				+"       ORGANIZATION_CODE LIKE '001011092%' OR"
				+"       ORGANIZATION_CODE LIKE '001011093%' OR"
				+"       ORGANIZATION_CODE LIKE '001011094%' OR"
				+"       ORGANIZATION_CODE LIKE '001011095%' OR ORGANIZATION_CODE = '001011') AND"
				+"       (ORGANIZATION_NAME NOT LIKE '%电销%' AND"
				+"       ORGANIZATION_NAME NOT LIKE '%养老金%' AND"
				+"       ORGANIZATION_NAME NOT LIKE '%分公司%' AND"
				+"       ORGANIZATION_NAME NOT LIKE '%95522%')";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "GWORG",DataFrameUtil.CACHETABLE_EAGER);
	}
	
	/*
	 * 被保人详情，投保人id
	 */
	private DataFrame getInsurantTmp(HiveContext sqlContext) {
		String hql = "SELECT /*+MAPJOIN(PO)*/ DISTINCT PL.POLICYHOLDER_ID,"  //投保人id
				+ "		UI.NAME AS INSURANT_NAME,"
				+ "		CEILING(2017 - SUBSTR(UI.BIRTHDAY,1,4)) AS USER_AGE,"     //被保人年龄
				+ "		CASE"
				+ "			WHEN UI.GENDER = '0' THEN"
				+ "				'男'"
				+ "			ELSE"
				+ "				'女' END AS GENDER,"
				+ "		PL.LRT_ID,"
				+ "		PL.LIA_ACCEPTTIME,"
				+ "		PL.HANDLE_CLIENTID"
				+ "	FROM P_LIFEINSURE PL"
				+ "	INNER JOIN P_INSURANT PI"
				+ "		ON PL.LIA_ID = PI.LIA_ID"
				+ " LEFT JOIN P_CUSTOMER UI"
				+ "		ON UI.CUSTOMER_ID = PI.CUSTOMER_ID"
				+ "	INNER JOIN GWORG PO"
				+ "		ON PL.HANDLE_CLIENTID = PO.ORGANIZATION_ID"
				+ "	WHERE CEILING(2017 - SUBSTR(UI.BIRTHDAY,1,4)) >= 0 AND CEILING(2017 - SUBSTR(UI.BIRTHDAY,1,4)) <= 12"
				+ "		AND UI.CUSTOMER_ID IS NOT NULL AND UI.CUSTOMER_ID <> ''";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_INSURANT");
	}
	
	
	private DataFrame getOtherInsurantTmp(HiveContext sqlContext) {
		String hql = "SELECT /*+MAPJOIN(PO)*/ DISTINCT PL.POLICYHOLDER_ID,"  //投保人id
				+ "		UI.NAME AS INSURANT_NAME,"
				+ "		CEILING(2017 - SUBSTR(UI.BIRTHDAY,1,4)) AS USER_AGE,"     //被保人年龄
				+ "		CASE"
				+ "			WHEN UI.GENDER = '0' THEN"
				+ "				'男'"
				+ "			ELSE"
				+ "				'女' END AS GENDER,"
				+ "		PL.LRT_ID,"
				+ "		PL.LIA_ACCEPTTIME,"
				+ "		PL.HANDLE_CLIENTID,"
				+ "		PO.ORGANIZATION_NAME"
				+ "	FROM P_LIFEINSURE PL"
				+ "	INNER JOIN P_INSURANT PI"
				+ "		ON PL.LIA_ID = PI.LIA_ID"
				+ " LEFT JOIN P_CUSTOMER UI"
				+ "		ON UI.CUSTOMER_ID = PI.CUSTOMER_ID"
				+ "	LEFT JOIN GWORG PO"
				+ "		ON PL.HANDLE_CLIENTID = PO.ORGANIZATION_ID"
				+ "	WHERE PO.ORGANIZATION_ID IS NULL"
				+ "		AND CEILING(2017 - SUBSTR(UI.BIRTHDAY,1,4)) >= 0 AND CEILING(2017 - SUBSTR(UI.BIRTHDAY,1,4)) <= 12"
				+ "		AND UI.CUSTOMER_ID IS NOT NULL AND UI.CUSTOMER_ID <> ''";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_OTHERINSURANT");
	}
	
	/*
	 * 0-12岁被保人的投保人id，泰康在线渠道
	 */
	private DataFrame getInsurantDetailTmp(HiveContext sqlContext) {
		String hql = "SELECT DISTINCT US.OPEN_ID,"
				+ "		TI.POLICYHOLDER_ID,"  //投保人id
				+ "		US.MEMBER_ID,"    //投保人member_id
				+ "		US.CID_NUMBER,"   //投保人身份证号
				+ "		US.NAME AS POLICYHOLDER_NAME,"
				+ "		US.MOBILE,"
				+ "		TI.INSURANT_NAME,"
				+ "		TI.USER_AGE,"
				+ "		TI.GENDER,"
				+ "		TI.LRT_ID,"
				+ "		TI.LIA_ACCEPTTIME"
				+ "	FROM TMP_INSURANT TI"
				+ "	INNER JOIN (SELECT * FROM FACT_USERINFO WHERE CUSTOMER_ID <> '' AND CUSTOMER_ID IS NOT NULL) US"
				+ "		ON US.CUSTOMER_ID = TI.POLICYHOLDER_ID"
				+ " left join tkoldb.wx_openid_nwq pc on pc.openid = us.open_id"
				+ " where pc.openid is null";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_IND");
	}
	
	
	private DataFrame getOtherInsurantDetailTmp(HiveContext sqlContext) {
		String hql = "SELECT DISTINCT US.OPEN_ID,"
				+ "		TI.POLICYHOLDER_ID,"  //投保人id
				+ "		US.MEMBER_ID,"    //投保人member_id
				+ "		US.CID_NUMBER,"   //投保人身份证号
				+ "		US.NAME AS POLICYHOLDER_NAME,"
				+ "		US.MOBILE,"
				+ "		TI.INSURANT_NAME,"
				+ "		TI.USER_AGE,"
				+ "		TI.GENDER,"
				+ "		TI.LRT_ID,"
				+ "		TI.LIA_ACCEPTTIME,"
				+ "		TI.ORGANIZATION_NAME"
				+ "	FROM TMP_OTHERINSURANT TI"
				+ "	INNER JOIN (SELECT * FROM FACT_USERINFO WHERE CUSTOMER_ID <> '' AND CUSTOMER_ID IS NOT NULL) US"
				+ "		ON US.CUSTOMER_ID = TI.POLICYHOLDER_ID"
				+ "	INNER JOIN " + sca_ol +"P_CLIENTORG PO"
				+ "		ON TI.HANDLE_CLIENTID = PO.ORGANIZATION_ID"
				+ " left join tkoldb.wx_openid_nwq pc on pc.openid = us.open_id"
				+ " where pc.openid is null";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_OTHERIND");
	}
	

	
	private DataFrame getInsureAllTMP(HiveContext sqlContext) {
		String hql = "SELECT DISTINCT TI.OPEN_ID,"
				+ " 		TI.POLICYHOLDER_ID,"
				+ "			DL.LRT_NAME AS PURCHASE_NAME,"
				+ " 		TI.MEMBER_ID,"
				+ "			TI.MOBILE,"
				+ "			TI.GENDER,"
				+ "			TI.USER_AGE,"
				+ "			SUBSTR(TI.LIA_ACCEPTTIME,1,10) AS ACCEPTTIME,"
				+ "			TI.INSURANT_NAME,"
				+ "			TI.POLICYHOLDER_NAME"
				+ "		FROM TMP_IND TI"
				+ " INNER JOIN "+ sca_ol + "D_LIFERISKTYPE DL"
				+ "		ON TI.LRT_ID = DL.LRT_ID"
				+ "	UNION ALL"
				+ "	SELECT DISTINCT TI.OPEN_ID,"
				+ "			TI.POLICYHOLDER_ID,"
				+ "			GG.RISKCNAME AS PURCHASE_NAME,"
				+ "			TI.MEMBER_ID,"
				+ "			TI.MOBILE,"
				+ "			TI.GENDER,"
				+ "			TI.USER_AGE,"
				+ "			SUBSTR(GP.ISSUEDATE,1,10) AS ACCEPTTIME,"
				+ "			TI.INSURANT_NAME,"
				+ "			TI.POLICYHOLDER_NAME"
				+ "		FROM GUPOLICYMAIN GP"
				+ "	INNER JOIN GUPOLICYRELATEDPARTY GU"
				+ "		ON GP.POLICYNO = GU.POLICYNO"
				+ "	INNER JOIN (SELECT * FROM TMP_IND WHERE CID_NUMBER <> '' AND CID_NUMBER IS NOT NULL) TI"
				+ "		ON TI.CID_NUMBER = case when GU.IDENTIFYNUMBER is null or GU.IDENTIFYNUMBER='' then genrandom('IDENTIFYNUMBER_') else GU.IDENTIFYNUMBER end "
				+ "     AND TI.POLICYHOLDER_NAME = GU.INSUREDNAME"
				+ "	INNER JOIN "+ sca_ol + "GGRISK GG"
				+ "		ON GG.RISKCODE = GP.PRODUCTCODE";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_INSURE");
	}
	
	private DataFrame getOtherInsureAllTMP(HiveContext sqlContext) {
		String hql = "SELECT DISTINCT TI.OPEN_ID,"
				+ " 		TI.POLICYHOLDER_ID,"
				+ "			DL.LRT_NAME AS PURCHASE_NAME,"
				+ " 		TI.MEMBER_ID,"
				+ "			TI.MOBILE,"
				+ "			TI.GENDER,"
				+ "			TI.USER_AGE,"
				+ "			SUBSTR(TI.LIA_ACCEPTTIME,1,10) AS ACCEPTTIME,"
				+ "			TI.INSURANT_NAME,"
				+ "			TI.POLICYHOLDER_NAME,"
				+ "			TI.ORGANIZATION_NAME"
				+ "			"
				+ "		FROM TMP_OTHERIND TI"
				+ " INNER JOIN "+ sca_ol + "D_LIFERISKTYPE DL"
				+ "		ON TI.LRT_ID = DL.LRT_ID"
				+ "	UNION ALL"
				+ "	SELECT DISTINCT TI.OPEN_ID,"
				+ "			TI.POLICYHOLDER_ID,"
				+ "			GG.RISKCNAME AS PURCHASE_NAME,"
				+ "			TI.MEMBER_ID,"
				+ "			TI.MOBILE,"
				+ "			TI.GENDER,"
				+ "			TI.USER_AGE,"
				+ "			SUBSTR(GP.ISSUEDATE,1,10) AS ACCEPTTIME,"
				+ "			TI.INSURANT_NAME,"
				+ "			TI.POLICYHOLDER_NAME,"
				+ "			TI.ORGANIZATION_NAME"
				+ "		FROM GUPOLICYMAIN GP"
				+ "	INNER JOIN GUPOLICYRELATEDPARTY GU"
				+ "		ON GP.POLICYNO = GU.POLICYNO"
				+ "	INNER JOIN (SELECT * FROM TMP_OTHERIND WHERE CID_NUMBER <> '' AND CID_NUMBER IS NOT NULL) TI"
				+ "		ON TI.CID_NUMBER = GU.IDENTIFYNUMBER"
				+ "	INNER JOIN "+ sca_ol + "GGRISK GG"
				+ "		ON GG.RISKCODE = GP.PRODUCTCODE";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_OTHERINSURE");
	}
	

	
	private DataFrame getGatherInsurantTmp(HiveContext sqlContext) {
		String hql = "SELECT TI.OPEN_ID,"
				+ "		TI.POLICYHOLDER_ID,"  //投保人id
				+ "		TI.MEMBER_ID,"    //投保人member_id
				+ "		TI.POLICYHOLDER_NAME AS USER_NAME,"
				+ "		MAX(TI.ACCEPTTIME)ACCEPTTIME,"
				+ "		TI.MOBILE,"
				+ "		CONCAT('被保人姓名：',"
				+ "				CONCAT_WS('||',COLLECT_SET(TI.INSURANT_NAME)),"
				+ "				'\073被保人年龄：',"
				+ "				CONCAT_WS('||',COLLECT_SET(CAST(TI.USER_AGE AS VARCHAR(10)))),"
				+ "				'\073被保人性别：',"
				+ "				CONCAT_WS('||',COLLECT_SET(TI.GENDER)),"
				+ "				'\073购买的时间：',"
				+ "				CONCAT_WS('||',COLLECT_SET(TI.ACCEPTTIME)),"
				+ "				'\073购买的产品：',"
				+ "				CONCAT_WS('||',COLLECT_SET(TI.PURCHASE_NAME)))"
				+ "		AS INSURANT_REMARK"
				+ "	FROM TMP_INSURE TI"
				+ "	GROUP BY TI.OPEN_ID,TI.POLICYHOLDER_ID,TI.MEMBER_ID,TI.POLICYHOLDER_NAME,TI.MOBILE";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_GATHER");
	}
	
	private DataFrame getThresholdTmp(HiveContext sqlContext,String year) {
		String hql = "SELECT DISTINCT TI.POLICYHOLDER_NAME AS USER_NAME,"
				+ "		MAX(TI.ACCEPTTIME) AS MAX_ACCEPTTIME"
				+ "	FROM TMP_OTHERINSURE TI"
				+ "	WHERE MAX(TI.ACCEPTTIME) <" + year
				+ "	GROUP BY TI.POLICYHOLDER_NAME";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_THRESHOLD");
	}
	
	private DataFrame getGatherOtherInsurantTmp(HiveContext sqlContext) {
		String hql = "SELECT DISTINCT TI.OPEN_ID,"
				+ "		TI.POLICYHOLDER_ID,"  //投保人id
				+ "		TI.MEMBER_ID,"    //投保人member_id
				+ "		TI.POLICYHOLDER_NAME AS USER_NAME,"
				+ "		TI.MOBILE,"
				+ "		CONCAT('被保人姓名：',"
				+ "				CONCAT_WS('||',COLLECT_SET(TI.INSURANT_NAME)),"
				+ "				'\073被保人年龄：',"
				+ "				CONCAT_WS('||',COLLECT_SET(CAST(TI.USER_AGE AS VARCHAR(10)))),"
				+ "				'\073被保人性别：',"
				+ "				CONCAT_WS('||',COLLECT_SET(TI.GENDER)),"
				+ "				'\073购买的时间：',"
				+ "				CONCAT_WS('||',COLLECT_SET(TI.ACCEPTTIME)),"
				+ "				'\073购买的产品：',"
				+ "				CONCAT_WS('||',COLLECT_SET(TI.PURCHASE_NAME)),"
				+ "				'\073购买的渠道：',"
				+ "				CONCAT_WS('||',COLLECT_SET(TI.ORGANIZATION_NAME)))"
				+ "		AS INSURANT_REMARK"
				+ "	FROM TMP_OTHERINSURE TI"
				+ "	GROUP BY TI.OPEN_ID,TI.POLICYHOLDER_ID,TI.MEMBER_ID,TI.POLICYHOLDER_NAME,TI.MOBILE";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_OTHERGATHER");
	}
	
	private DataFrame getOtherInsurantResult(HiveContext sqlContext) {
		String hql = "SELECT TI.OPEN_ID,TI.POLICYHOLDER_ID,TI.MEMBER_ID,TI.USER_NAME,TI.MOBILE,TI.INSURANT_REMARK"
				+ "	FROM TMP_OTHERGATHER TI"
				+ "	INNER JOIN TMP_THRESHOLD TT"
				+ "		ON TT.USER_NAME = TI.USER_NAME";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_INSURANTRESULT");
	}



	
	private DataFrame getChildrenCareClueTmp(HiveContext sqlContext) {
		String hql = "SELECT DISTINCT PI.OPEN_ID,"
				+ "			CASE"
				+ "				WHEN PI.MEMBER_ID <> '' AND PI.MEMBER_ID IS NOT NULL THEN"
				+ " 				PI.MEMBER_ID"
				+ "				WHEN PI.POLICYHOLDER_ID <> '' AND PI.POLICYHOLDER_ID IS NOT NULL THEN"
				+ " 				PI.POLICYHOLDER_ID"
				+ "				ELSE"
				+ " 				PI.MOBILE END AS USER_ID,"
				+ "			CASE"
				+ "				WHEN PI.MEMBER_ID <> '' THEN"
				+ "					'MEM'"
				+ "				WHEN PI.POLICYHOLDER_ID <> '' AND PI.POLICYHOLDER_ID IS NOT NULL THEN"
				+ "					'C'"
				+ " 			ELSE"
				+ "					'TELE' END AS USER_TYPE,"
				+ " 		CONCAT(NVL(CC.PRODUCT_NAME,''),NVL(CC.PRODUCT_NAME,'')) PRODUCT_NAME,"
				+ "			PI.USER_NAME,"
				+ "			PI.INSURANT_REMARK"
				+ " 	FROM TMP_GATHER PI"
				+ "		LEFT JOIN TMP_CC CC"
				+ "			ON PI.OPEN_ID = CC.OPEN_ID"
				+ "		LEFT JOIN TMP_CC TT"
				+ "			ON PI.MEMBER_ID = TT.OPEN_ID";
		
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_CHILDRENCAREALL");
	}
	
	private DataFrame getOtherChildrenCareClueTmp(HiveContext sqlContext) {
		String hql = "SELECT DISTINCT PI.OPEN_ID,"
				+ "			CASE"
				+ "				WHEN PI.MEMBER_ID <> '' AND PI.MEMBER_ID IS NOT NULL THEN"
				+ " 				PI.MEMBER_ID"
				+ "				WHEN PI.POLICYHOLDER_ID <> '' AND PI.POLICYHOLDER_ID IS NOT NULL THEN"
				+ " 				PI.POLICYHOLDER_ID"
				+ "				ELSE"
				+ " 				PI.MOBILE END AS USER_ID,"
				+ "			CASE"
				+ "				WHEN PI.MEMBER_ID <> '' THEN"
				+ "					'MEM'"
				+ "				WHEN PI.POLICYHOLDER_ID <> '' AND PI.POLICYHOLDER_ID IS NOT NULL THEN"
				+ "					'C'"
				+ " 			ELSE"
				+ "					'TELE' END AS USER_TYPE,"
				+ " 		CC.PRODUCT_NAME,"
				+ "			PI.USER_NAME,"
				+ "			PI.INSURANT_REMARK"
				+ " 	FROM TMP_INSURANTRESULT PI"
				+ "		LEFT JOIN TMP_CC CC"
				+ "			ON PI.OPEN_ID = CC.OPEN_ID"
				+ "		LEFT JOIN TMP_CC TT"
				+ "			ON PI.MEMBER_ID = TT.OPEN_ID";
		
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_OTHERCHILDRENCAREALL");
	}


	
	
	private DataFrame getCCtaticEventTmp(HiveContext sqlContext, String appids) {
    	String timeStamp = Long.toString(getTodayTime(0) / 1000);
		String quarterTimeStamp = Long.toString(getTodayTime(-90) / 1000);
    	String hql = "SELECT  USER_ID AS OPEN_ID,"
    			+ "			  APP_TYPE,"
    			+ "			  APP_ID,"
    			+ "			  EVENT,"
    			+ "			  LABEL"
                + " FROM  F_STATISTICS_EVENT " 
                + " WHERE APP_TYPE IN ('webSite', 'H5','app') AND APP_ID IN"
                + " (" + appids +")"
                + " AND USER_ID IS NOT NULL AND USER_ID <> ''"
                + " AND EVENT = '商品详情'"
    		    + " AND  from_unixtime(cast(cast(VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') >= from_unixtime(cast(" + quarterTimeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')"
    			+ " AND  from_unixtime(cast(cast(VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') < from_unixtime(cast(" + timeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')";
        
    	return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_SRC");
    }
	
	
	
	public DataFrame getChildrenCareClue(HiveContext sqlContext) {
		String hql = "SELECT OPEN_ID,"
				+ "			USER_ID,"
				+ "			USER_TYPE,"
				+ "			CONCAT('投保人姓名：',"
				+ " 			NVL(USER_NAME,' '),"
				+ "				'\073近三个月浏览产品名称：',"
				+ " 			CONCAT_WS('||',COLLECT_SET(PRODUCT_NAME)),"
				+ "				'\073',INSURANT_REMARK) AS REMARK,"
				+ " 		USER_NAME"
				+ " 	FROM TMP_CHILDRENCAREALL"
				+ "	GROUP BY OPEN_ID,USER_ID,USER_TYPE,INSURANT_REMARK,USER_NAME";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_CHILDRENCARE");
		
	}
	
	public DataFrame getOtherChildrenCareClue(HiveContext sqlContext) {
		String hql = "SELECT OPEN_ID,"
				+ "			USER_ID,"
				+ "			USER_TYPE,"
				+ "			CONCAT('投保人姓名：',"
				+ " 			NVL(USER_NAME,' '),"
				+ "				'\073近三个月浏览产品名称：',"
				+ " 			CONCAT_WS('||',COLLECT_SET(PRODUCT_NAME)),"
				+ "				'\073',INSURANT_REMARK) AS REMARK,"
				+ " 		USER_NAME"
				+ " 	FROM TMP_OTHERCHILDRENCAREALL"
				+ "	GROUP BY OPEN_ID,USER_ID,USER_TYPE,INSURANT_REMARK,USER_NAME";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_CHILDRENCARE");
		
	}
	
	public DataFrame getChildrenCareClueResult(HiveContext sqlContext) {
		String hql = "SELECT '' AS ROWKEY,"
				+ "			USER_ID,"
				+ "			USER_TYPE,"
				+ "			'' AS APP_TYPE,"
				+ "			'once_childcare_1' AS APP_ID,"
				+ " 		'' AS EVENT_TYPE,"
				+ "			'被保险人为子女' AS EVENT,"
				+ "			'' AS SUB_TYPE,"
				+ "			'' AS VISIT_DURATION,"
				+ "			'' AS FROM_ID,"
				+ "			'' AS PAGE_TYPE,"
				+ "			'客户经营营销活动' AS FIRST_LEVEL,"
				+ "			'预测式外拨数据' AS SECOND_LEVEL,"
				+ " 		'被保险人为子女' AS THIRD_LEVEL,"
				+ " 		'被保险人为子女' AS FOURTH_LEVEL,"
				+ "			'' AS VISIT_TIME,"
				+ " 		'' AS VISIT_COUNT,"
				+ " 		'1' AS CLUE_TYPE,"
				+ "			REMARK,"
				+ " 		USER_NAME"
				+ " 	FROM TMP_CHILDRENCARE";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "CHILDRENCARE");
		
	}
	
	public DataFrame getOtherChildrenCareClueResult(HiveContext sqlContext) {
		String hql = "SELECT '' AS ROWKEY,"
				+ "			USER_ID,"
				+ "			USER_TYPE,"
				+ "			'' AS APP_TYPE,"
				+ "			'once_childcare_2' AS APP_ID,"
				+ " 		'' AS EVENT_TYPE,"
				+ "			'商品详情' AS EVENT,"
				+ "			'' AS SUB_TYPE,"
				+ "			'' AS VISIT_DURATION,"
				+ "			'' AS FROM_ID,"
				+ "			'' AS PAGE_TYPE,"
				+ "			'客户经营营销活动' AS FIRST_LEVEL,"
				+ "			'预测式外拨数据' AS SECOND_LEVEL,"
				+ " 		'被保险人为子女' AS THIRD_LEVEL,"
				+ " 		'被保险人为子女' AS FOURTH_LEVEL,"
				+ "			'' AS VISIT_TIME,"
				+ " 		'' AS VISIT_COUNT,"
				+ " 		'1' AS CLUE_TYPE,"
				+ "			REMARK,"
				+ " 		USER_NAME"
				+ " 	FROM TMP_CHILDRENCARE";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "OTHERCHILDRENCARE");
		
	}
	
	
	
	public JavaRDD<FactChildrenCare> analysisFields(DataFrame df) {
		JavaRDD<Row> jRdd = df.select("OPEN_ID", "APP_TYPE","APP_ID","EVENT","LABEL").rdd().toJavaRDD();
		JavaRDD<FactChildrenCare> pRdd = jRdd.map(new Function<Row, FactChildrenCare>() {



			private static final long serialVersionUID = 1L;

			@Override
			public FactChildrenCare call(Row v1) throws Exception {
				String open_id = v1.getString(0);
				String app_type = v1.getString(1);
				String app_id = v1.getString(2);
				String event = v1.getString(3);
				String label = v1.getString(4);
				String product_name = "";
				
				if(label.contains("combocode")) {
					Pattern p1 = Pattern.compile("(lrtName:\")(.+)(\",combocode)");
					Matcher m1 = p1.matcher(label);
					if(m1.find()) {
						product_name = m1.group(2);
					}
					
				}
				else {
					Pattern p1 = Pattern.compile("(productName:\")(.+)(\",seedId)");
					Matcher m1 = p1.matcher(label);
					if(m1.find()) {
						product_name = m1.group(2);
					}
				}
				return new FactChildrenCare(open_id,app_type,app_id,event,product_name);
			}
			
			
		});
		return pRdd;
		
	}
	
	

}
