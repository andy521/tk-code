package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.desttable.FactUserBehaviorClue;
import com.tk.track.fact.sparksql.main.App;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.fact.sparksql.util.DateTimeUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

/**
 * @author itw_zhaocq
 * @Date:2016年10月10日
 */
public class EStation2HomeWeb implements Serializable{

	private static final long serialVersionUID = 5102937514585457445L;
	public static final String HIVE_SCHEMA = TK_DataFormatConvertUtil.getSchema();
	/**
	 * 
	 * @Description: 获取e站到家行为线索
	 * @param sqlContext
	 * @return
	 * @author zhao.chq
	 * @date 2016年9月27日
	 */
	public JavaRDD<FactUserBehaviorClue> getEStation2HomeWeb(HiveContext sqlContext){
		DataFrame df = getDataFrame(sqlContext);
		JavaRDD<FactUserBehaviorClue> pRDD = df.toJavaRDD()
				.filter(new Function<Row,Boolean>(){
					private static final long serialVersionUID = -6203488147136073069L;
					@Override
					public Boolean call(Row v1) throws Exception {
						if (v1.getString(7).trim().replace("->", "").length() == 0){
							return false;
						}
						return true;
					}

				})
				.map(new Function<Row,FactUserBehaviorClue>(){

					private static final long serialVersionUID = -1513043002056316601L;
					@Override
					public FactUserBehaviorClue call(Row row) throws Exception {
						String ROWKEY = "";
						String USER_ID = row.getString(0);
						String USER_TYPE = row.getString(1);
						String APP_TYPE = row.getString(2);
						String APP_ID = row.getString(3);
						String EVENT = row.getString(4);
						String EVENT_TYPE = row.getString(5);
						String SUB_TYPE = row.getString(6);
						String VISIT_DURATION = row.getString(7);
						String FROM_ID = row.getString(8);
						String USER_NAME = row.getString(9);
						String PAGE_TYPE = row.getString(10);
						String FIRST_LEVEL = row.getString(11);
						String SECOND_LEVEL = row.getString(12);
						String THIRD_LEVEL = row.getString(13);
						String FOURTH_LEVEL = row.getString(14);
						String VISIT_TIME = row.getString(15);
						String VISIT_COUNT = row.getString(16);
						String CLUE_TYPE = row.getString(17);
						String REMARK = row.getString(18);
						return new FactUserBehaviorClue(ROWKEY,USER_ID,USER_TYPE,APP_TYPE,APP_ID,EVENT_TYPE, EVENT, SUB_TYPE, VISIT_DURATION,FROM_ID, PAGE_TYPE, FIRST_LEVEL, SECOND_LEVEL, THIRD_LEVEL, FOURTH_LEVEL, VISIT_TIME, VISIT_COUNT, CLUE_TYPE, REMARK, USER_NAME);
					}

				});
		return pRDD;
	}
	
	public DataFrame getDataFrame(HiveContext sqlContext){
		loadFactUserInfo(sqlContext);
		loadStaticEvent(sqlContext);
		loadPolicyInfoResultTable(sqlContext);
		load_User_Relation(sqlContext);
		getUserInfoFull(sqlContext);
		getDataEventInc(sqlContext);
		
		AppendCustomerIDByMemberID(sqlContext);
		AppendPolicyInfoByCustomerID_LifeProcess1(sqlContext);
		AppendPolicyInfoByCustomerID_LifeProcess2(sqlContext);
		 //saveAsParquet(df,"/user/tkonline/taikangtrack_test/data/estation-parquet");

		AppendRelationInfoByCustomerID_Process1(sqlContext);
		AppendRelationInfoByCustomerID_Process2(sqlContext);
		AppendRelationInfoByCustomerID_Process3(sqlContext);
		 //saveAsParquet(df1,"/user/tkonline/taikangtrack_test/data/relation-parquet");
		
		CombinePolicyAndRelation(sqlContext);
		GetDataFromByEVENT(sqlContext);
		GetDataFromBySubType(sqlContext);
		//return null
		GetResultData(sqlContext);
		return getWhClueData(sqlContext);
	}
    
    /*
     * load大健康ETL APP1 FACT_USER_INFO 
     * */
    public static void loadFactUserInfo(HiveContext hiveContext){
    	String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_USERINFO_INPUTPATH);
		if (!TK_DataFormatConvertUtil.isExistsPath(path)){
			System.out.println("ERROR---------" + path + "-----IS NOT EXIST------");
			System.exit(0);
		}
		//register table
		hiveContext.load(path).registerTempTable("FACT_USER_INFO_ESTATION");
    }
    
    /*
     * load用户行为F_STATICT_EVENT_LOG 
     * */
    public static void loadStaticEvent(HiveContext hiveContext){
    	String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH);
		if (!TK_DataFormatConvertUtil.isExistsPath(path)){
			System.out.println("ERROR---------" + path + "-----IS NOT EXIST------");
			System.exit(0);
		}
		//register table
		hiveContext.load(path).registerTempTable("FACT_STATISTICS_EVENT_TOTAL");
    }
    
    /*
     * load寿险有效保单信息表 FACT_NET_POLICYRESULTINFO
     * */
    public void loadPolicyInfoResultTable(HiveContext sqlContext) {
    	String sysdt = App.GetSysDate(-1);//currentDay
    	String pathTemp = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTNETPOLICYINFORESULT_OUTPUTPATH);
    	String path = pathTemp + "-" + sysdt;
    	if (TK_DataFormatConvertUtil.isExistsPath(path)) {
	    	sqlContext.load(path).registerTempTable("POLICYINFORESULT"); //有效保单
    	}else{
			System.out.println("ERROR---------" + path + "-----IS NOT EXIST------");
			System.exit(0);
    	}
    }
    
    /*
     * load家族关系表 FACT_NET_RELATION
     * */
    public void load_User_Relation(HiveContext sqlContext) {
    	String sysdt = App.GetSysDate(-1);//currentDay
    	String pathTemp = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTNETRELATION_OUTPUTPATH);
    	String path = pathTemp + "-" + sysdt;
    	if (TK_DataFormatConvertUtil.isExistsPath(path)) {
	    	sqlContext.load(path).registerTempTable("FACT_NET_RELATION"); //有效保单
    	}else{
			System.out.println("ERROR---------" + path + "-----IS NOT EXIST------");
			System.exit(0);
    	}
    }
    
    /*
     * load寿险产品配置表 D_LIFERISKTYPE
     * */
    private static void load_D_LIFERISKTYPE(HiveContext sqlContext) {                                                                                                                                           
    	String hql = "SELECT * "
    			+ "FROM   "+HIVE_SCHEMA+"D_LIFERISKTYPE ";
    	DataFrameUtil.getDataFrame(sqlContext, hql, "D_LIFERISKTYPE",DataFrameUtil.CACHETABLE_EAGER);
    }
    
    /*
     * load财险产品配置表 GGRISK
     * */
    private static void load_GGRISK(HiveContext sqlContext) {                                                                                                                                           
    	String hql = "SELECT * "
    			+ "FROM   "+HIVE_SCHEMA+"GGRISK ";
    	DataFrameUtil.getDataFrame(sqlContext, hql, "GGRISK",DataFrameUtil.CACHETABLE_EAGER);
    }
    
    /*
     * 获取customer或者memberid 不为空的字段
     * */
    private static void getUserInfoFull(HiveContext hiveContext) {                                                                                                                                           
    	String sql =  "SELECT DISTINCT "
    			+ "	MEMBER_ID,CUSTOMER_ID, OPEN_ID, MOBILE, NAME, CIDTYPE_ID, CID_NUMBER,USER_AGE, "
    			+ " GENDER,BIRTHDAY,USER_VOCATION,USER_ADDRESS,USER_MARITAL_STATUS,USER_HAS_CHILD,USER_ANNUAL_INCOME "
    			+ " FROM FACT_USER_INFO_ESTATION "
    			+ "	WHERE CUSTOMER_ID IS NOT NULL AND MEMBER_ID IS NOT NULL AND CUSTOMER_ID <> '' AND MEMBER_ID <> '' ";
    	DataFrameUtil.getDataFrame(hiveContext, sql, "FACT_USER_INFO_FULL");
    }
    
    
    /*
     * 获取前一天的F_STATICT_EVENT_LOG中e站到家增量数据 
     * */
    public  void getDataEventInc(HiveContext hiveContext){
    	
    	String timeStamp = Long.toString(DateTimeUtil.getTodayTime(0) / 1000);
		String yesterdayTimeStamp = Long.toString(DateTimeUtil.getTodayTime(-1) / 1000);

    	String sql = "SELECT "
    			+ " USER_ID, APP_ID, APP_TYPE, CUSTOM_VAL, EVENT,"
    			+ " SUBTYPE, LABEL, VISIT_COUNT, VISIT_DURATION, VISIT_TIME, FROM_ID "
    			+ " FROM FACT_STATISTICS_EVENT_TOTAL TB1"
    			+ " WHERE "
    			+ "	APP_ID = 'javaWeb002' "
    			+ " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') >= from_unixtime(cast(" + yesterdayTimeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')"
    			+ " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') < from_unixtime(cast(" + timeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')";
   
		DataFrameUtil.getDataFrame(hiveContext, sql, "FACT_STATISTICS_EVENT_INC");
    }
    
    /*
     * 通过memberID关联FACT_USER_INFO补全CustomerID等用户信息 
     * 
     */
    public static void AppendCustomerIDByMemberID(HiveContext hiveContext){
       	String sql = "SELECT /*+MAPJOIN(STATIC_INC)*/"
       			+ " DISTINCT"
    			+ " TRIM(STATIC_INC.USER_ID) AS USER_ID, "
    			+ "	TRIM(STATIC_INC.APP_ID) AS APP_ID, "
    			+ "	TRIM(STATIC_INC.APP_TYPE) AS APP_TYPE, "
    			+ "	TRIM(STATIC_INC.CUSTOM_VAL) AS CUSTOM_VAL, "
    			+ "	TRIM(STATIC_INC.EVENT) AS EVENT,"
    			+ " TRIM(STATIC_INC.SUBTYPE) AS SUBTYPE, "
    			+ "	TRIM(STATIC_INC.LABEL) AS LABEL, "
    			+ "	TRIM(STATIC_INC.VISIT_COUNT) AS VISIT_COUNT, "
    			+ "	TRIM(STATIC_INC.VISIT_DURATION) AS VISIT_DURATION, "
    			+ "	TRIM(STATIC_INC.VISIT_TIME) AS VISIT_TIME, "
    			+ "	TRIM(STATIC_INC.FROM_ID) AS FROM_ID,  "
    			+ " TRIM(F_USERINFO.MOBILE) AS MOBILE, "
    			+ " TRIM(F_USERINFO.NAME) AS NAME, "
    			+ " TRIM(F_USERINFO.CIDTYPE_ID) AS CIDTYPE_ID, "
    			+ " TRIM(F_USERINFO.CID_NUMBER) AS CID_NUMBER, "
    			+ " TRIM(F_USERINFO.USER_AGE) AS USER_AGE, "
    			+ " TRIM(F_USERINFO.BIRTHDAY) AS BIRTHDAY, "
    			+ " TRIM(F_USERINFO.GENDER) AS GENDER, "
				+ " TRIM(F_USERINFO.USER_HAS_CHILD) AS USER_HAS_CHILD, "
				+ " TRIM(F_USERINFO.USER_VOCATION) AS USER_VOCATION, "
				+ " TRIM(F_USERINFO.USER_MARITAL_STATUS) AS USER_MARITAL_STATUS, "
				+ " TRIM(F_USERINFO.USER_ANNUAL_INCOME) AS USER_ANNUAL_INCOME, "
    			+ " TRIM(F_USERINFO.CUSTOMER_ID) AS CUSTOMER_ID "
    			+ " FROM FACT_STATISTICS_EVENT_INC STATIC_INC"
    			+ " LEFT JOIN FACT_USER_INFO_FULL F_USERINFO "
				+ " ON STATIC_INC.USER_ID = F_USERINFO.MEMBER_ID ";
       	
		DataFrameUtil.getDataFrame(hiveContext, sql, "USER_ESTATION_APPEND_CUSTOMERID",DataFrameUtil.CACHETABLE_PARQUET);
    }
    
    /*
     * 通过customerID补全
     * 1. 寿险保单信息  多个保单合并到一个字段
     * 2. 财险保单信息暂未补全
     * */
    public DataFrame AppendPolicyInfoByCustomerID_LifeProcess1(HiveContext hiveContext){
       	String sql = "SELECT "
       			+ "	ES_CUSTOMER.* ,"
    			+ " CONCAT_WS(',',"
    			+ " CONCAT('投保人姓名：',TRIM(POLICYINFO.POLICYHOLDER_NAME)),"
    			//+ "	POLICYINFO.POLICYHOLDER_ID,"
    			+ "	CONCAT('保单号：',TRIM(POLICYINFO.LIA_POLICYNO)),"
    			+ "	CONCAT('险种名称：',TRIM(POLICYINFO.LRT_NAME)),"
    			+ "	CONCAT('有效期开始时间：',TRIM(POLICYINFO.LIA_VALIDPERIODBEGIN)) "
    			//+ "	CONCAT('有效期预期结束时间：',TRIM(POLICYINFO.LIA_VALIDPERIODWILLEND)) "
    			+ " ) AS LIFE_POLICYINFO_SUB "
    			+ " FROM USER_ESTATION_APPEND_CUSTOMERID ES_CUSTOMER "
    			+ " INNER JOIN POLICYINFORESULT  POLICYINFO "
				+ " ON (case when ES_CUSTOMER.CUSTOMER_ID = '' or ES_CUSTOMER.CUSTOMER_ID is null then genrandom('TKCustom')"
				+ "			 else ES_CUSTOMER.CUSTOMER_ID end) = POLICYINFO.POLICYHOLDER_ID ";
		return DataFrameUtil.getDataFrame(hiveContext, sql, "USER_ESTATION_APPEND_POLICYINFO_P1");
    }
    
    public DataFrame AppendPolicyInfoByCustomerID_LifeProcess2(HiveContext hiveContext){
       	String sql = "SELECT "
       			+ " USER_ID, APP_ID, APP_TYPE, CUSTOM_VAL, EVENT,"
    			+ " SUBTYPE, LABEL, VISIT_COUNT, VISIT_DURATION, VISIT_TIME, FROM_ID, "
    			+ " CUSTOMER_ID,MOBILE,NAME,CIDTYPE_ID,CID_NUMBER,USER_AGE,BIRTHDAY,USER_HAS_CHILD,USER_VOCATION,USER_MARITAL_STATUS,USER_ANNUAL_INCOME, "
    			+ " GENDER,"
    			+ " TRIM(CONCAT_WS('|',COLLECT_SET(LIFE_POLICYINFO_SUB))) AS LIFE_POLICYINFO "
    			+ " FROM USER_ESTATION_APPEND_POLICYINFO_P1 "
    			+ " GROUP BY "
       			+ " USER_ID, APP_ID, APP_TYPE, CUSTOM_VAL, EVENT,"
    			+ " SUBTYPE, LABEL, VISIT_COUNT, VISIT_DURATION, VISIT_TIME, FROM_ID, "
    			+ " CUSTOMER_ID,MOBILE,NAME,CIDTYPE_ID,CID_NUMBER,USER_AGE,BIRTHDAY,USER_HAS_CHILD,USER_VOCATION,USER_MARITAL_STATUS,USER_ANNUAL_INCOME, "
    			+ " GENDER";
		return DataFrameUtil.getDataFrame(hiveContext, sql, "USER_ESTATION_APPEND_POLICYINFO");
    }
    
    /*
     * 通过customerID补全家庭关系信息  多个合并
     * 
     * */
    public DataFrame AppendRelationInfoByCustomerID_Process1(HiveContext hiveContext){
       	String sql = "SELECT DISTINCT "
       			+ "	UE.CUSTOMER_ID,"
       			+ " UE.USER_ID,"
       			+ " RLE.RL_CUSTOMER_ID, "
       			+"(case RLE.RELATIONID_INSURANT "
       					+" when 1 then "
       					+"'本人'"
       					+" when 2 then "
       					+"'丈夫'"
       					+" when 3 then "
       					+"'妻子'"
       					+" when 4 then "
       					+"'父亲'"
       					+" when 5 then "
       					+"'母亲'"
       					+" when 6 then "
       					+"'儿子'"
       					+" when 7 then "
       					+"'女儿'"
       					+" when 8 then "
       					+"'祖父'"
       					+" when 9 then "
       					+"'祖母'"
       					+" when 10 then "
       					+"'孙子'"
       					+" when 11 then "
       					+"'孙女'"
       					+" when 12 then "
       					+"'外祖父'"
       					+" when 13 then "
       					+"'外祖母'"
       					+" when 14 then "
       					+"'外孙'"
       					+" when 15 then "
       					+"'外孙女'"
       					+" when 16 then "
       					+"'哥哥'"
       					+" when 17 then "
       					+"'姐姐'"
       					+" when 18 then "
       					+"'弟弟'"
       					+" when 19 then "
       					+"'妹妹'"
       					+" when 20 then "
       					+"'公公'"
       					+" when 21 then "
       					+"'婆婆'"
       					+" when 22 then "
       					+"'儿媳'"
       					+" when 23 then "
       					+"'岳父'"
       					+" when 24 then "
       					+"'岳母'"
       					+" when 25 then "
       					+"'女婿'"
       					+" when 26 then "
       					+"'其它亲属'"
       					+" when 27 then "
       					+"'同事'"
       					+" when 28 then "
       					+"'朋友'"
       					+" when 29 then "
       					+"'雇主'"
       					+" when 30 then "
       					+"'其它'"
       					+" when 31 then "
       					+"'法定'"
       					+" when 89 then "
       					+"'父母'"
       					+" when 90 then "
       					+"'子女'"
       					+" when 91 then "
       					+"'祖父母'"
       					+" when 92 then "
       					+"'孙辈'"
       					+" when 93 then "
       					+"'外祖父母'"
       					+" when 94 then "
       					+"'外孙辈'"
       					+" when 95 then "
       					+"'哥哥姐姐'"
       					+" when 96 then "
       					+"'弟弟妹妹'"
       					+" when 97 then "
       					+"'公婆'"
       					+" when 98 then "
       					+"'岳父母'"
       					+" when 99 then "
       					+"'雇员'"
       					+" else "
       					+"'其他'"
       					+ " end) as RL_INFO "
    			+ " FROM USER_ESTATION_APPEND_CUSTOMERID UE"
    			+ " INNER JOIN FACT_NET_RELATION RLE"
    			+ " ON (case when UE.CUSTOMER_ID = '' or UE.CUSTOMER_ID is null then genrandom('TKCustom')"
				+ "			 else UE.CUSTOMER_ID end) = RLE.CUSTOMER_ID "
    			+ "	GROUP BY UE.CUSTOMER_ID,UE.USER_ID,RLE.RL_CUSTOMER_ID,RLE.RELATIONID_INSURANT ";
       	return DataFrameUtil.getDataFrame(hiveContext, sql, "RELATION_CUSTOMERID");
    }
    
    public DataFrame AppendRelationInfoByCustomerID_Process2(HiveContext hiveContext){
       	String sql = "SELECT  /*+MAPJOIN(T1)*/ "
       			+ " DISTINCT "
       			+ "	T1.CUSTOMER_ID,"
    			+ " CONCAT_WS(',',"
    			+ " CONCAT('MEMBER_ID:',TRIM(T2.MEMBER_ID)),"
    			+ "	CONCAT('姓名：',TRIM(T2.NAME)),"
    			+ "	CONCAT('关系：',TRIM(T1.RL_INFO)),"
    			//+ "	TRIM(T2.CIDTYPE_ID),"
    			//+ "	TRIM(T2.CID_NUMBER),"
    			//+ "	TRIM(T2.MOBILE),"
    			+ "	CONCAT('性别：',case TRIM(T2.GENDER) when '0' then '男' when '1' then '女' else '未知' end),"
    			+ "	CONCAT('年龄：',TRIM(T2.USER_AGE)), "
    			+ "	CONCAT('证件归属地：',TRIM(T2.MOBILE_PROVINCE),TRIM(MOBILE_CITY)) "
    			+ " ) AS RELATION_INFO "
    			+ " FROM RELATION_CUSTOMERID T1"
    			+ " LEFT JOIN FACT_USER_INFO_ESTATION T2"
				+ " ON (case when T2.CUSTOMER_ID = '' or T2.CUSTOMER_ID is null then genrandom('TKCustom')"
				+ "			 else T2.CUSTOMER_ID end) = TRIM(T1.RL_CUSTOMER_ID) ";
		return DataFrameUtil.getDataFrame(hiveContext, sql, "RELATION_CUSTOMER_INFO_P1");
    }
    
    public DataFrame AppendRelationInfoByCustomerID_Process3(HiveContext hiveContext){
       	String sql = "SELECT "
       			+ "	P1.CUSTOMER_ID ,"
    			+ " CONCAT_WS('|',COLLECT_SET(TRIM(P1.RELATION_INFO))) AS RELATION_INFO "
    			+ " FROM RELATION_CUSTOMER_INFO_P1 P1 "
				+ " GROUP BY P1.CUSTOMER_ID ";
       	return DataFrameUtil.getDataFrame(hiveContext, sql,"RELATION_CUSTOMER_INFO_P2");
    }
    
    /*
     * 保单信息及家庭信息合并
     * */
    public static void CombinePolicyAndRelation(HiveContext hiveContext){
       	String sql = "SELECT DISTINCT "
       			+ " P1.*, "
    			+ " P2.RELATION_INFO AS RELATION_INFO "
    			+ " FROM USER_ESTATION_APPEND_POLICYINFO P1 "
				+ " LEFT JOIN RELATION_CUSTOMER_INFO_P2 P2 "
				+ " ON P2.CUSTOMER_ID = P1.CUSTOMER_ID  ";
		DataFrameUtil.getDataFrame(hiveContext, sql, "USER_ESTATION_RESULT_INFO");
    }
    
    /*
     * 筛选结果表数据
     * */
    public void GetDataFromByEVENT(HiveContext hiveContext){
    	String sql = "SELECT "
    			+ " CUSTOMER_ID,NAME,MOBILE,CID_NUMBER,CIDTYPE_ID,"
    			+ " GENDER,BIRTHDAY,USER_AGE,"
    			+ " USER_ID, APP_TYPE, APP_ID, EVENT,"
    			+ " SUBTYPE, LABEL, VISIT_COUNT, VISIT_TIME, VISIT_DURATION, FROM_ID, CUSTOM_VAL, "
    			+ "	LIFE_POLICYINFO, "
    			+ " RELATION_INFO "
    			+ " FROM USER_ESTATION_RESULT_INFO "
    			+ " WHERE "
    			+ initEStationEventConfigTable();
		DataFrameUtil.getDataFrame(hiveContext, sql, "USER_ESTATION_BY_EVENT");
    }
    
    public  DataFrame GetDataFromBySubType(HiveContext hiveContext){
    	String sql = "SELECT "
    			+ " CUSTOMER_ID,NAME,MOBILE,CID_NUMBER,CIDTYPE_ID,"
    			+ " GENDER,BIRTHDAY,USER_AGE,"
    			+ " USER_ID, APP_TYPE, APP_ID, EVENT,"
    			+ " SUBTYPE, LABEL, VISIT_COUNT, VISIT_TIME, VISIT_DURATION, FROM_ID, CUSTOM_VAL, "
    			+ "	LIFE_POLICYINFO, "
    			+ " RELATION_INFO "
    			+ " FROM USER_ESTATION_BY_EVENT "
    			+ " WHERE "
    			+ initEStationSubTypeConfigTable();
    	return DataFrameUtil.getDataFrame(hiveContext, sql, "USER_ESTATION_BY_SUBTYPE");
    	
    }
    
    public  DataFrame GetResultData(HiveContext hiveContext){
    	String sql = "SELECT DISTINCT "
    			+ " CUSTOMER_ID AS CUSTOMERID,"
    			+ " USER_ID AS USERID,"
    			+ " NAME AS NAME,"
    			+ " (case TRIM(GENDER) when '0' then '男' when '1' then '女' else '未知' end) AS GENDER,"
    			+ " USER_AGE AS USER_AGE,"
    			+ " BIRTHDAY AS BIRTHDAY,"
    			+ " 'e站到家' AS APP_ID,"
    			+ " CONCAT_WS('|',COLLECT_SET(CONCAT(EVENT,'->',SUBTYPE))) AS TRACK_INFO, "
    			+ " CAST(SUM(CAST(VISIT_COUNT AS INT)) AS STRING) AS VISIT_COUNT,"
    			+ " CAST(from_unixtime(cast(cast(COLLECT_SET(VISIT_TIME)[0] as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') as string) AS VISIT_TIME,"
    			+ " CAST(SUM(CAST(VISIT_DURATION AS INT)) AS STRING) AS VISIT_DURATION,"
    			+ "	LIFE_POLICYINFO AS LIFE_POLICYINFO, "
    			+ " RELATION_INFO AS RELATION_INFO, "
    			+ " CONCAT_WS('|',COLLECT_SET(LABEL)) AS LABEL "
    			+ " FROM USER_ESTATION_RESULT_INFO "
    			+ " GROUP BY "
    			+ " CUSTOMER_ID,USER_ID,NAME,"
    			//+ " MOBILE,CID_NUMBER,CIDTYPE_ID,APP_TYPE,"
    			+ " GENDER,USER_AGE,BIRTHDAY,"
    			+ " LIFE_POLICYINFO,RELATION_INFO";
    	return DataFrameUtil.getDataFrame(hiveContext, sql, "RESULT_INFO_TABLE");    	
    }
    
    public  DataFrame getWhClueData(HiveContext hiveContext){
    	String sql = "SELECT "
    			   + "	USERID AS USER_ID,"	
    			   + "	'MEM' AS USER_TYPE,"	
    			   + "  'javaWeb' AS APP_TYPE,"
    			   + "  'estation_javaWeb002' AS APP_ID,"
    			   + "  'trace' AS EVENT,"
    			   + "  'E站到家轨迹' AS EVENT_TYPE,"
    			   + "  '' AS SUB_TYPE,"
    			   + "  VISIT_DURATION,"
    			   + "  '3' AS FROM_ID,"
    			   + "  NAME AS USER_NAME,"
    			   + "  '服务' AS PAGE_TYPE,"
    			   + "  '武汉营销数据' AS FIRST_LEVEL,"
    			   + "  '官网会员' AS SECOND_LEVEL,"
    			   + "  'e站到家获取' AS THIRD_LEVEL,"
    			   + "  'e站到家' AS FOURTH_LEVEL,"
    			   + "  VISIT_TIME,"
    			   + "  VISIT_COUNT,"
    			   + "  '2' AS CLUE_TYPE,"
    			   + "  CONCAT('姓名：',NVL(NAME,''),'\073客户号：',NVL(CUSTOMERID,''),'\073性别：',NVL(GENDER,''),'\073出生日期：',NVL(BIRTHDAY,''),'\073年龄：',NVL(USER_AGE,''),'\073浏览轨迹：',NVL(TRACK_INFO,''),'\073寿险保单信息：',NVL(LIFE_POLICYINFO,''),'\073家族关系信息：',NVL(RELATION_INFO,''),'\073浏览信息：',NVL(LABEL,'')) AS REMARK"
    			   + " FROM RESULT_INFO_TABLE";	
    	return DataFrameUtil.getDataFrame(hiveContext, sql, "WH_CLUE_DATA");    	
    }
    
    
	/**
	 * Event 过滤条件
	 */
    public String initEStationEventConfigTable() {
    	
 		String[] keys = new String[]{"投连产品","在线申请","投资产品专区","万能产品","业务办理","在线投保","我要推荐","投连交易记录查询","意外险卡单激活"};
 		String value = "";
 		for (int i = 0;i < keys.length;i++){
 			if(i > 0){
 				value += " OR ";
 			}
 			value += "EVENT LIKE " + " '%" + keys[i] + "%' ";
 		}
 		System.out.println(value);
 		return value;
     }
     
	/**
	 * SubType 过滤条件
	 */
     public String initEStationSubTypeConfigTable() {
 		
 		String[] keys = new String[]{"投连追加投资","新增可选保额","量身定做计划","身故可选保额","新增或取消重疾附加险","追加保险费","追加保费","追加投资","续期缴费","在线投保","卡单激活"};
 		String value = "";
 		for (int i = 0;i < keys.length;i++){
 			if(i > 0){
 				value += " OR ";
 			}
 			value += "SUBTYPE LIKE " + " '%" + keys[i] + "%' ";
 		}
 		System.out.println(value);
 		return value;
     }
    
	/**
	 * 解析Lable policyNo:"210315046450,210315046451,210315057461"
	 */
	public  Map<String,String> analysisLabel(String Label) {
		
		String policyNo = "";//--保单号
		String info = "";//--成功或失败详情 
		String total = "";//-_tl:代表投连产品、_wn:代表万能产品、null或空为在线申请
		String product = "";//--不固定参数，区分是万能产品内万能险追加投资功能还是业务办理内保单信息变更功能
		String flag = "";
		
		Map<String,String> dataMap = new HashMap<String, String>(); 
		
		//to do 以后根据具体需求设定 end
		Pattern p = Pattern.compile("(.*):\"(.*)\"");		
		String  regex = "\",";
		String  substr = "\";";
		Label = Label.replaceAll(regex, substr);// ,->;
		String[] data = Label.split(";");
		
		for(int i = 0; i < data.length; i++) {
			String REMARK = "";
			Matcher m = p.matcher(data[i]);
			if (m.find()) {
				if (m.group(1).equalsIgnoreCase("policyNo")) {
					REMARK = m.group(2).trim()==null?"无":m.group(2).trim();
					policyNo = "关联保单号："+REMARK;
					dataMap.put("policyNo",policyNo);
					System.out.println(policyNo);
				}
				if (m.group(1).equalsIgnoreCase("info")){
					REMARK = m.group(2).trim()==null?"无":m.group(2).trim();
					info = "追加成功或失败详情："+REMARK;
					dataMap.put("info",info);
					System.out.println(info);
				}
				if (m.group(1).equalsIgnoreCase("total")) {
					REMARK = m.group(2).trim()==null?"无":m.group(2).trim();
					total = "追加总金额："+REMARK;
					dataMap.put("total",total);
					System.out.println(total);
				}
				if (m.group(1).equalsIgnoreCase("product")){
					REMARK = m.group(2).trim()==null?"无":m.group(2).trim();
					product = "产品类型："+REMARK;//-_tl:代表投连产品、_wn:代表万能产品、null或空为在线申请
					dataMap.put("product",product);
					System.out.println(product);
				}
				if (m.group(1).equalsIgnoreCase("flag")){
					REMARK = m.group(2).trim()==null?"无":m.group(2).trim();
					flag = "不固定参数："+REMARK;//--不固定参数，区分是万能产品内万能险追加投资功能还是业务办理内保单信息变更功能
					dataMap.put("flag",flag);
					System.out.println(flag);
				}
			}
			System.out.println(REMARK);
		}
		return dataMap;
	}
	
	
	public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<FactUserBehaviorClue> rdd, String path) {
		if (TK_DataFormatConvertUtil.isExistsPath(path)) {
			//TK_DataFormatConvertUtil.deletePath(path);
			sqlContext.createDataFrame(rdd, FactUserBehaviorClue.class).save(path, "parquet", SaveMode.Append);
		} else {
			sqlContext.createDataFrame(rdd, FactUserBehaviorClue.class).saveAsParquetFile(path);
		}
		
	}
    
    public void saveAsParquet(DataFrame df, String path) {
		TK_DataFormatConvertUtil.deletePath(path);
		df.saveAsParquetFile(path);
	}
}
