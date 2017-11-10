package com.tk.track.fact.sparksql.etl;


import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.desttable.RecomendWithoutAlgoResultBean;
import com.tk.track.fact.sparksql.desttable.RecommendWithoutAlgoMidBean;
import com.tk.track.fact.sparksql.desttable.RecommendWithoutAlgoScore;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecomengWithoutAlgo implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 9007285493470056100L;
	private static final String bhSchema = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HIVE_USERBEHAVIOUR_SCHEMA);
	private static final String days = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_RECOMEND_USERINFO_TIMERECENT);
	private static final String percent = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_RECOMEND_SCORE_PERCENT);
	private static final String wechatPMap = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_RECOMEND_WECHAT_PRODUCTMAP);
	private static final String wechatEventPMap = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_RECOMEND_WECHAT_EVENTPRODUCTMAP);
	public static final String defaultReNum= TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_DEFAULT_RECOMMENDNUM);
	public static final String wapResultPath=TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_SCORE_WAP);
	public static final String pcResultPath=TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_SCORE_PC);
	public static final String appResultPath=TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_SCORE_APP);

	public DataFrame getRecommendDataResultDF(HiveContext sqlContext) {
		
		//合并匿名用户和登录用户的统计
		DataFrame dfJoin=joinFsEvent(sqlContext);
//		saveAsParquet(dfJoin, "/user/tkonline/taikangtrack_test/data/fact_statics_dfJoin");
		
		//得到随机化后的大健康表,防止后面数据倾斜
		DataFrame FactHealthcreditscoreresult=getFactHealthcreditscoreresultNotNullDF(sqlContext);
//		saveAsParquet(FactHealthcreditscoreresult, "/user/tkonline/taikangtrack_test/data/FactHealthcreditscoreresult");
		//pc端
		DataFrame df_pc=getPcJoinProductUserLogInfo(sqlContext);
//		saveAsParquet(df_pc, "/user/tkonline/taikangtrack_test/data/fact_statics_df_pc");
		//微端
		getWechatUserLogInfo(sqlContext);
		DataFrame dfwx=getOpenIdAndUserID(sqlContext,"wx_UserBrowseInfo","wx_UserBrowseInfo_openid");
//		saveAsParquet(dfwx, "/user/tkonline/taikangtrack/data/fact_statics_dfwx");

		//wap
		DataFrame dfwap=getWapUserLogInfo(sqlContext);
//		saveAsParquet(dfwap, "/user/tkonline/taikangtrack_test/data/fact_statics_dfwap");
		//app
		DataFrame dfapp=getAppUserLogInfo(sqlContext);
//		saveAsParquet(dfapp, "/user/tkonline/taikangtrack_test/data/fact_statics_dfapp");


		//后面不再使用,删除缓存
		sqlContext.uncacheTable("F_STATISTICS_EVENT_ALL");
		//调优。大健康表数据多, 合并三端数据后再与大健康表关联,减少关联次数
		MergePcAndAppAndWap(sqlContext);



		//用合并后的三端数据与大健康表关联,减少关联次数
		getMemberIdAndUserID(sqlContext,"MergePcAndAppAndWap","merge_UserBrowseInfo_memberid");
		//合并通用userid
		DataFrame dfmerge=mergeRecommendDataResultDF(sqlContext);

//		saveAsParquet(dfmerge, "/user/tkonline/taikangtrack_test/data/fact_statics_dfmerge");
		
//		DataFrame dfproduct=GetProductName(sqlContext,"UserBrowseInfo");
//		saveAsParquet(dfproduct, "/user/tkonline/taikangtrack/data/fact_statics_dfproduct");
		/*
		 * 有userID的以userid统计,terminal_id不需要,现在为空
		 */
		getLifeInsureCustomerId(sqlContext);
		getVaildPolicy(sqlContext);
		DataFrame df=getUserBrowseAndPolicyInfo(sqlContext,"UserBrowseInfo");
		return df;
	}

	/**
	 * 大健康表数据多
	 * 合并三端数据后再与大健康表关联,减少关联次数
	 * @param sqlContext
	 * @return
	 */
	private DataFrame MergePcAndAppAndWap(HiveContext sqlContext) {
		String sql = "SELECT t1.terminal_id," +
				"t1.app_id," +
				"t1.event," +
				"t1.user_id," +
				"t1.visit_duration," +
				"t1.visit_count," +
				"t1.visit_time," +
				"t1.lrt_id," +
				"t1.unique_pid," +
				"t1.label "
				+ "	from  wap_UserBrowseInfo t1 " +
				"union all " +
				"SELECT t2.terminal_id," +
				"t2.app_id," +
				"t2.event," +
				"t2.user_id," +
				"t2.visit_duration," +
				"t2.visit_count," +
				"t2.visit_time," +
				"t2.lrt_id," +
				"t2.unique_pid," +
				"t2.label "
				+ "	from  app_UserBrowseInfo t2   " +
				"union all " +
				"SELECT t3.terminal_id," +
				"t3.app_id," +
				"t3.event," +
				"t3.user_id," +
				"t3.visit_duration," +
				"t3.visit_count," +
				"t3.visit_time," +
				"t3.lrt_id," +
				"t3.unique_pid," +
				"t3.label "
				+ "	from  pc_UserBrowseInfo t3  ";

		return DataFrameUtil.getDataFrame(sqlContext, sql, "MergePcAndAppAndWap", DataFrameUtil.CACHETABLE_EAGER);
	}
    /**
     * 合并各个端结果 
     * @param sqlContext
     * @return
     */
	private DataFrame mergeRecommendDataResultDF(HiveContext sqlContext) {
//		String sql = "SELECT pc.* "
//			+ "	from pc_UserBrowseInfo_memberid pc "
//			+ "	union all "
//			+ "	SELECT wechat.* "
//			+ "	from wx_UserBrowseInfo_openid wechat "
//			+ "	union all "
//			+ "	SELECT app.* "
//			+ "	from app_UserBrowseInfo_memberid app "
//			+ "	union all "
//			+ "	SELECT wap.* "
//			+ "	from wap_UserBrowseInfo_memberid wap ";
		String sql ="SELECT wechat.* "
			+ "	from wx_UserBrowseInfo_openid wechat "
			+ "	union all "
			+ "	SELECT merge.* "
			+ "	from merge_UserBrowseInfo_memberid merge "
			;
		return DataFrameUtil.getDataFrame(sqlContext, sql, "UserBrowseInfo");
	}
	
	public DataFrame getFactHealthcreditscoreresultNotNullDF(HiveContext sqlContext) {
		String sql = "SELECT CASE "
					+ "        WHEN MEMBER_ID = '' OR MEMBER_ID is null "
		            + "          THEN  genrandom('MEMBER_ID_') "
					+ "      ELSE MEMBER_ID END AS MEMBER_ID, "
				    + "      CASE "
					+ "        WHEN OPEN_ID = '' OR OPEN_ID is null "
		            + "          THEN  genrandom('OPEN_ID_') "
					+ "      ELSE OPEN_ID END AS OPEN_ID, "
				    + "	USER_ID "
					+ "	FROM fact_healthcreditscoreresult";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "FactHealthcreditscoreResultUserIdNotNullDF");
//		return DataFrameUtil.getDataFrame(sqlContext, sql, "FactHealthcreditscoreResultUserIdNotNullDF", DataFrameUtil.CACHETABLE_PARQUET);
	}
	
	/**
	 * 当源表中user_id 为memberId 时关联fact_healthcreditscoreresult 取通用的 userid,原userId作为memberid
	 * @param sqlContext
	 * @param tableName
	 * @return
	 */
	private DataFrame getMemberIdAndUserID(HiveContext sqlContext,String tableName,String destTableName) {
		String sql = "SELECT /*+MAPJOIN(orgtable)*/ orgtable.terminal_id,h.user_id,orgtable.user_id member_id,'' as open_id,orgtable.lrt_id,orgtable.unique_pid,orgtable.app_id,orgtable.event,orgtable.visit_duration,orgtable.visit_count,orgtable.visit_time,orgtable.label "
			+ "	from "+tableName+" orgtable left join FactHealthcreditscoreResultUserIdNotNullDF h"
			+ "	on orgtable.user_id=h.member_id";
			
		return DataFrameUtil.getDataFrame(sqlContext, sql, destTableName);
	}
	/**
	 * 当源表中user_id 为open_id 时关联fact_healthcreditscoreresult 取通用的 userid,原userId作为open_id
	 * @param sqlContext
	 * @param tableName
	 * @return
	 */
	private DataFrame getOpenIdAndUserID(HiveContext sqlContext,String tableName,String destTableName) {
		String sql = "SELECT /*+MAPJOIN(orgtable)*/ orgtable.terminal_id,h.user_id,'' as member_id,orgtable.user_id open_id,orgtable.lrt_id,orgtable.unique_pid,orgtable.app_id,orgtable.event,orgtable.visit_duration,orgtable.visit_count,orgtable.visit_time,orgtable.label "
			+ "	from "+tableName+" orgtable left join FactHealthcreditscoreResultUserIdNotNullDF h"
			+ "	on orgtable.user_id=h.open_id";
			
		return DataFrameUtil.getDataFrame(sqlContext, sql, destTableName);
	}
	
	
	/**
	 * 微信用户浏览行为数据
	 * @param sqlContext
	 * @return 
	 */
	private DataFrame getWechatUserLogInfo(HiveContext sqlContext) {
//		String sql = "SELECT    t1.terminal_id,t1.app_id,  t1.event,  t1.user_id,  t1.visit_duration,  t1.visit_count,from_unixtime(cast(cast(t1.visit_time as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') as visit_time,  regexp_extract(t1.label,'lrt_id:([0-9]+)') as lrt_id,t1.label "
//				+ "FROM F_STATISTICS_EVENT_ALL t1   where t1.app_id = 'wechat005'   and t1.event in "
//				+ "('9.9元戒烟首页', '90天戒烟门诊首页', '戒烟药品首页', '泰康蒲公英定期寿险',  '门急诊住院报销', '丁香园门诊详情页面')  and "
//				+ "(t1.subtype = '' or t1.subtype is null)    "
//				+" and datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'),from_unixtime(cast(cast(t1.visit_time as bigint) / 1000 as bigint),'yyyy-MM-dd')) <= "+days+" "
//				+ "union all  select   "
//				+ "t2.terminal_id,t2.app_id,  t2.event,  t2.user_id,  t2.visit_duration,  t2.visit_count, from_unixtime(cast(cast(t2.visit_time as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') as visit_time,  regexp_extract(t2.label,'lrt_id:([0-9]+)') as lrt_id, t2.label from "
//				+ "F_STATISTICS_EVENT_ALL t2  where "
//				+ "t2.app_id = 'wechat005'  and t2.event in ('信息填写')  and "
//				+ "(t2.subtype = '' or t2.subtype is null)  and t2.label like '%1103%'"
//				+" and datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'),from_unixtime(cast(cast(t2.visit_time as bigint) / 1000 as bigint),'yyyy-MM-dd')) <= "+days+" ";

		String caseWhen1=getCaseProductId(wechatPMap, "subtype");
		String caseWhen2=getCaseProductId(wechatEventPMap, "event");

		String sql="select terminal_id,app_id,user_id,visit_count,visit_duration,visit_time,event,lrt_id,lrt_id as unique_pid,label from " +
				"(" +
				"select terminal_id,app_id,user_id,visit_count,visit_duration,from_unixtime(cast(cast(visit_time as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') as visit_time,event,label, " +
//				"case  " +
//				" when subtype='AI情预报' then 'S20150012'    " +
//				" when subtype='加班保障' then '9904'    " +
//				" when subtype='喜临门家财保障' then 'S20150008'    " +
//				" when subtype='福临门家庭财产安全保障' then 'S20150001'    " +
//				" when subtype='9.9元科学戒烟管理计划' then 'S20160073'    " +
//				" when subtype='体检保' then 'S20160091'    " +
//				" when subtype='少儿重疾' then 'S20160159'       " +
//				" when subtype='微互助求关爱' then '241'     " +
//				" when subtype='综合意外' then '239'     " +
//				" when subtype='老年癌症医疗保险' then 'S20160266'      " +
//				" when subtype='e享健康返本型重疾险' then '298'     " +
//				" when subtype='住院保' then 'S20160203'    " +
//				" when subtype='E理财B投连险' then '522'    " +
//				" when subtype='万里无忧B款' then '297'    " +
//				" when subtype='健康1+1返本型重疾险' then '290'    " +
//				" when subtype='悦享中华高端医疗费用报销' then '249'  " +
//				" else ''    " +
//				" end as lrt_id    " +
				caseWhen1+" "+
				"from F_STATISTICS_EVENT_ALL  where app_id ='wechat005' and event <> 'page.load'  " +
				"and subtype in('健康1+1返本型重疾险', 'AI情预报','悦享中华高端医疗费用报销','住院保','老年癌症医疗保险','综合意外','万里无忧B款','E理财B投连险','加班保障','喜临门家财保障','福临门家庭财产安全保障','微互助求关爱','e享健康返本型重疾险','少儿重疾','体检保','9.9元科学戒烟管理计划')  " +
				" and datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'),from_unixtime(cast(cast(visit_time as bigint) / 1000 as bigint),'yyyy-MM-dd')) <= "+days+" "+
				" union all  " +
				"select terminal_id,app_id,user_id,visit_count,visit_duration,from_unixtime(cast(cast(visit_time as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') as visit_time,event,label,   " +
//				"case  " +
//				" when event='门急诊住院报销' then '282'    " +
//				" when event='定期寿险' then '281'  " +
//				" when event='短期意外险首页' then '1103'   " +
//				" else ''   " +
//				"end as lrt_id  " +
				caseWhen2+" "+
				"from F_STATISTICS_EVENT_ALL where app_id ='wechat005' and event <> 'page.load' and  " +
				"event in ('门急诊住院报销','定期寿险','短期意外险首页')  " +
				" and datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'),from_unixtime(cast(cast(visit_time as bigint) / 1000 as bigint),'yyyy-MM-dd')) <= "+days+" "+
				" union all  " +
				" select terminal_id,app_id,user_id,visit_count,visit_duration,from_unixtime(cast(cast(visit_time as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') as visit_time,event,label, " +
				"'S20160158' as lrt_id " +
				" from F_STATISTICS_EVENT_ALL where  " +
				" app_id = 'wechat_insure_flow' and label like '%S20160158%'  " +
				" and event <> 'page.load' "+
				" and datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'),from_unixtime(cast(cast(visit_time as bigint) / 1000 as bigint),'yyyy-MM-dd')) <= "+days+" " +
				") ba ";
		
		return DataFrameUtil.getDataFrame(sqlContext, sql, "wx_UserBrowseInfo");
	}
	
	/**
	 * wap用户浏览行为数据
	 * @param sqlContext
	 * @return 
	 */
		private DataFrame getWapUserLogInfo(HiveContext sqlContext) {
			String sql = "SELECT  " + 
					"t.terminal_id,"
					+ "t.app_id, " + 
					"t.event,  " + 
					"t.user_id,  " + 
					"t.visit_duration, " + 
					"t.visit_count, " + 
					"from_unixtime(cast(cast(t.visit_time as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') as visit_time, " + 
					"regexp_extract(t.label,'lrtID:\"([0-9]+)\"') as lrt_id," +
					"regexp_extract(LABEL,'(?<=combocode:\\\")[^\\\",]*',0) as unique_pid,t.label " +
					"FROM  F_STATISTICS_EVENT_ALL t " + 
					"where app_id = 'clue_H5_mall_001' and event = '商品详情'"+
					" and datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'),from_unixtime(cast(cast(t.visit_time as bigint) / 1000 as bigint),'yyyy-MM-dd')) <= "+days+"" +
					" union all " +
					"SELECT  " +
					"t.terminal_id,"
					+ "t.app_id, " +
					"t.event,  " +
					"t.user_id,  " +
					"t.visit_duration, " +
					"t.visit_count, " +
					"from_unixtime(cast(cast(t.visit_time as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') as visit_time, " +
					"regexp_extract(t.label,'productId:\"(S[0-9]+)\"') as lrt_id," +
					"regexp_extract(LABEL,'(?<=productId:\\\")[^\\\",]*',0) as unique_pid," +
					"t.label " +
					"FROM  F_STATISTICS_EVENT_ALL t " +
					"where app_id = 'H5_insure_flow' and event = '商品详情'"+
					" and datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'),from_unixtime(cast(cast(t.visit_time as bigint) / 1000 as bigint),'yyyy-MM-dd')) <= "+days+"";
			return DataFrameUtil.getDataFrame(sqlContext, sql, "wap_UserBrowseInfo");
		}

		/**
		 * App用户浏览行为数据
		 * @param sqlContext
		 * @return 
		 */
		private DataFrame getAppUserLogInfo(HiveContext sqlContext) {
			String sql = "SELECT  "
					+ "t.terminal_id," + 
					"t.app_id, " + 
					"t.event, " + 
					"t.user_id, " + 
					"t.visit_duration, " + 
					"t.visit_count, " + 
					"from_unixtime(cast(cast(t.visit_time as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') as visit_time, " + 
					"split(t.subType,' ')[0] as lrt_id,split(t.subType,' ')[0] as unique_pid,t.label " +
					"FROM  F_STATISTICS_EVENT_ALL t " + 
					"where app_id = 'app001' and event = 'detail' "+
					" and datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'),from_unixtime(cast(cast(t.visit_time as bigint) / 1000 as bigint),'yyyy-MM-dd')) <= "+days+" ";
			return DataFrameUtil.getDataFrame(sqlContext, sql, "app_UserBrowseInfo");
		}
	
	
	/**
	 * 1 Pc用户浏览信息数据
	 * @param sqlContext
	 * @return 
	 */
	private DataFrame getPcJoinProductUserLogInfo(HiveContext sqlContext) {
		String sql = "select fs.terminal_id,fs.app_id,fs.event,fs.user_id,fs.visit_duration,fs.visit_count,fs.visit_time," +
				"fs.lrt_id,fs.lrt_id unique_pid,fs.label from (" +
				"select terminal_id,app_id,event,user_id,visit_duration,from_unixtime(cast(cast(visit_time as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') as visit_time,visit_count," + 
				"regexp_extract(label,'code:\"([0-9]+)\"') as lrt_id,label from F_STATISTICS_EVENT_ALL where app_id = 'mall001' and event = '商品详情'" + 
				" and datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'),from_unixtime(cast(cast(visit_time as bigint) / 1000 as bigint),'yyyy-MM-dd')) <= "+days+" "+
				") fs" ;
		return DataFrameUtil.getDataFrame(sqlContext, sql, "pc_UserBrowseInfo");
	}
	/**
	 * 同时关联寿险和财险产品配置表格 得到两个表的产品名称（没有为空）
	 *  按lrt_id是否是S开头 区分出寿险和财险产品,使用对应的产品名称
	 * @param
	 * @return
	 */
	private DataFrame GetProductName(HiveContext sqlContext,String tableName){
//		String sql="select had1.app_id,had1.event,had1.user_id,had1.visit_duration,had1.visit_count,had1.visit_time,had1.label,had1.lrt_id," + 
//				" case when had1.lrt_id Like 'S%' then plan.splanname  else lift.lrt_name end as productName from " + 
//				tableName+" had1 left join tkoldb.d_liferisktype lift on had1.lrt_id=lift.lrt_id left join tkubdb.gschannelplancode plan on had1.lrt_id=plan.splancode";
//				
		String sql="select /*+MAPJOIN(p1)*/ " +
				"had1.app_id, " +
				"had1.event," + 
				"had1.terminal_id," + 
				"had1.user_id," + 
				"had1.member_id," + 
				"had1.open_id," + 
				"had1.visit_duration," + 
				"had1.visit_count," + 
				"had1.visit_time," + 
				"had1.label," + 
				"had1.lrt_id, " + 
				"p1.lrt_name as productName " + 
				"from "+tableName+" had1 " + 
				"inner join tkoldb.d_liferisktype p1 on had1.lrt_id = p1.lrt_id " + 
				"UNION ALL " + 
				"select /*+MAPJOIN(p2)*/ had1.app_id, " +
				"had1.event, " + 
				"had1.terminal_id," + 
				"had1.user_id," + 
				"had1.member_id," + 
				"had1.open_id," + 
				"had1.visit_duration, " + 
				"had1.visit_count, " + 
				"had1.visit_time, " + 
				"had1.label, " + 
				"had1.lrt_id, " + 
				"p2.splanname as productName " + 
				"from "+tableName+" had1 " + 
				"inner join "+bhSchema+"gschannelplancode p2 on had1.lrt_id = p2.splancode";
		
		return DataFrameUtil.getDataFrame(sqlContext, sql, "UserBrowseProductInfo");	
		
	}
//	/**
//	 * 2  关联uba_log_event得到 terminal_id
//	 * @param sqlContext
//	 * @param tableName
//	 * @return
//	 */
//	private DataFrame getUserTerminalBrowseInfo(HiveContext sqlContext,String tableName) {
//		String sql = "select logevent.terminal_id,ubinfo.user_id,ubinfo.visit_duration,ubinfo.visit_count,"
//				+ "ubinfo.visit_time,ubinfo.lrt_id,ubinfo.productName"
//				+ " from "+tableName+" ubinfo,UBA_LOG_EVENT logevent "
//				+ "where ubinfo.app_id=logevent.app_id and ubinfo.event=logevent.event and from_unixtime(cast(cast(logevent.time as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss')=ubinfo.visit_time and logevent.label=ubinfo.label";
//		return DataFrameUtil.getDataFrame(sqlContext, sql, "UserTerminalBrowseInfo");
//	}
//	
	/**
	 * 3 关联用户浏览信息和有效保单信息表
	 * 找出用户浏览信息和对应的保单信息
	 * @param sqlContext
	 * @param tableName
	 * @return
	 */
	public DataFrame getUserBrowseAndPolicyInfo(HiveContext sqlContext,String tableName) {

		String sql="select terminal_id,user_id,member_id,open_id,visit_duration,visit_count,visit_time,lrt_id,unique_pid,isbuy,isvalid from ( " +
				"select  user_log_info.terminal_id, " +
				"user_log_info.user_id, " + 
				"user_log_info.member_id," + 
				"user_log_info.open_id,"+
				"user_log_info.visit_duration, " + 
				"user_log_info.visit_count, " + 
				"user_log_info.visit_time, " + 
				"user_log_info.lrt_id," +
				"user_log_info.unique_pid, " +
				"CASE " + 
				"WHEN cp.lia_policyno = '' OR cp.lia_policyno IS NULL THEN " + 
				"'false' " + 
				"ELSE " + 
				"'true' " + 
				"END AS isbuy, " + 
				"CASE " + 
				"WHEN cp.lia_policyno = '' OR cp.lia_policyno IS NULL THEN " + 
				"'false' " + 
				"ELSE " + 
				"'true' " + 
				"END AS isvalid " +
				"from "+tableName+"  as user_log_info " + 
				"LEFT JOIN UserVaildPolicy cp on cp.LRT_ID = " +
				"user_log_info.lrt_id " + 
				"and user_log_info.member_id = cp.member_id " +
				"where user_log_info.lrt_id is not null and user_log_info.lrt_id<>'' and " +
				"user_log_info.member_id is not null and user_log_info.member_id <>'' and " +
				"user_log_info.user_id is not null and user_log_info.user_id <>'' " +

				"union all "+

				"select  user_log_info.terminal_id, " +
				"user_log_info.user_id, " +
				"user_log_info.member_id," +
				"user_log_info.open_id,"+
				"user_log_info.visit_duration, " +
				"user_log_info.visit_count, " +
				"user_log_info.visit_time, " +
				"user_log_info.lrt_id," +
				"user_log_info.unique_pid, " +
				"CASE " +
				"WHEN cp.lia_policyno = '' OR cp.lia_policyno IS NULL THEN " +
				"'false' " +
				"ELSE " +
				"'true' " +
				"END AS isbuy, " +
				"CASE " +
				"WHEN cp.lia_policyno = '' OR cp.lia_policyno IS NULL THEN " +
				"'false' " +
				"ELSE " +
				"'true' " +
				"END AS isvalid " +
				"from "+tableName+"  as user_log_info " +
				"LEFT JOIN UserVaildPolicy cp on cp.LRT_ID = user_log_info.lrt_id " +
				"and user_log_info.open_id = cp.open_id " +
				"where user_log_info.lrt_id is not null and user_log_info.lrt_id<>'' and " +
				"user_log_info.open_id is not null and user_log_info.open_id <>'' and  " +
				"user_log_info.user_id is not null and user_log_info.user_id <>'' " +

				"union all "+

				"select  user_log_info.terminal_id, " +
				"user_log_info.user_id, " +
				"user_log_info.member_id," +
				"user_log_info.open_id,"+
				"user_log_info.visit_duration, " +
				"user_log_info.visit_count, " +
				"user_log_info.visit_time, " +
				"user_log_info.lrt_id," +
				"user_log_info.unique_pid, " +
				" 'false' as isbuy,"+
				" 'false' as isvalid "+
				"from "+tableName+"  as user_log_info " +
				"where user_log_info.lrt_id is not null and user_log_info.lrt_id<>'' and  " +
				"user_log_info.terminal_id is not null and user_log_info.terminal_id <>'' and " +
				"(user_log_info.user_id is null or user_log_info.user_id ='')" +
				") tt group by " +
				"terminal_id,user_id,member_id,open_id,visit_duration,visit_count,visit_time,lrt_id,unique_pid,isbuy,isvalid" ;

		return DataFrameUtil.getDataFrame(sqlContext, sql, "RBASE_USERBROWSEANDPOLICYINFO", DataFrameUtil.CACHETABLE_PARQUET);

	}
	/**
	 * FACT_USERINFO 用户表中有客户有两条及以上记录,需要合并,因后面以customer_id关联寿险保单,
	 * 所以以customer_id聚合,对于有多个会员号的人,按会员号降序排序,取最新的一条记录做为此人的记录。
	 * @param sqlContext
	 * @return
	 */
	private DataFrame getLifeInsureCustomerId(HiveContext sqlContext){
		String sql="select a.user_id_list[0] as user_id,member_id_list[0] as member_id,open_id_list[0] as open_id,customer_id from (" +
				"select collect_list(user_id)  user_id_list,collect_list(member_id) member_id_list, customer_id, " +
				"collect_list(open_id) open_id_list from (" +
				"select user_id,member_id,customer_id,open_id from FACT_USERINFO where member_id is not null and member_id <>'' " +
				" and customer_id is not null and customer_id<>''  order by member_id desc " +
				") t group by customer_id " +
				") a";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "USER_INFO_UNIQUE");
	}
	public DataFrame getVaildPolicy(HiveContext sqlContext){

		String sql="select uinfo.customer_id," +
				"uinfo.member_id," +
				"uinfo.open_id," +
				"fp.LRT_ID," +
				"fp.lia_policyno," +
				"fp.lia_validperiodbegin " +
				"from USER_INFO_UNIQUE uinfo " +
				"inner join (select LRT_ID," +
				"policyholder_id," +
				"lia_policyno," +
				"lia_validperiodbegin " +
				"from FactPInfoResult " +

				"where policyholder_id is not null " +
				" and LRT_ID is not null) fp on fp.policyholder_id =uinfo.customer_id";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "UserVaildPolicy");
	}
	/*
	 * 得到有userId的和Userid为空的匿名用户的 总统计表
	 */
	private DataFrame joinFsEvent(HiveContext sqlContext){
		String sql="SELECT " + 
				" TERMINAL_ID,"
		                + "USER_ID," 
		                + "APP_TYPE," 
		                + "APP_ID,"
		                + "EVENT," 
		                + "SUBTYPE,"
		                + "LABEL,"
		                + "CUSTOM_VAL,"
		                + "VISIT_COUNT,"
		                + "VISIT_TIME,"
		                + "VISIT_DURATION"
				+ " FROM TMP_FACT_STATISTICS_EVENT WHERE TERMINAL_ID <>'' AND TERMINAL_ID IS NOT NULL "
				+ " UNION ALL "
				+ "SELECT "
				+" '' AS TERMINAL_ID,"
                + "USER_ID," 
                + "APP_TYPE," 
                + "APP_ID,"
                + "EVENT," 
                + "SUBTYPE,"
                + "LABEL,"
                + "CUSTOM_VAL,"
                + "VISIT_COUNT,"
                + "VISIT_TIME,"
                + "VISIT_DURATION"
				+ " FROM F_STATISTICS_EVENT ";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "F_STATISTICS_EVENT_ALL", DataFrameUtil.CACHETABLE_EAGER);
	}
	public DataFrame countTotalDefaultProductPv(HiveContext sqlContext){
//		App.loadParquet(sqlContext, "/user/tkonline/taikangtrack/data/recommondwithoutalgo-20170527", "RBASE_USERBROWSEANDPOLICYINFO", false);
		countTotalProductPv(sqlContext);
		//关联微信端配置表,拼接
		getTotalWxProductPv(sqlContext);
		getTotalWxContactProductInfo(sqlContext);
		DataFrame df=getTotalProductResult(sqlContext);
//		saveAsParquet(df, "/user/tkonline/taikangtrack_test/data/recommengwioutalgo_dfwx");

		//关联Wap 配置表,拼接默认排名
		getTotalWapProductPv(sqlContext);
		getTotalWapContactProductInfo(sqlContext);
		DataFrame dfwap=getTotalWapProductResult(sqlContext);
//		saveAsParquet(dfwap, "/user/tkonline/taikangtrack_test/data/recommengwioutalgo_dfwap");

        // 关联pc配置表 拼接默认排名
		//计算pc配置表
		getTotalPcProduct(sqlContext);
		getTotalPcProductPv(sqlContext);
		getTotalPcContactProductInfo(sqlContext);
		DataFrame dfpc=getTotalPcProductResult(sqlContext);
		//saveAsParquet(dfpc, "/user/tkonline/taikangtrack_test/data/recommengwioutalgo_dfpc");
	  
		//关联app的配置表，拼接默认排名
		DataFrame dfapp11=getTotalAppProductPv(sqlContext);
//		saveAsParquet(dfapp11, "/user/tkonline/taikangtrack_test/data/getTotalAppProductPv");
		DataFrame dfapp112=getTotalAppContactProductInfo(sqlContext);
//		saveAsParquet(dfapp112, "/user/tkonline/taikangtrack_test/data/getTotalAppContactProductInfo");
		DataFrame dfapp=getTotalAppProductResult(sqlContext);
//		saveAsParquet(dfapp, "/user/tkonline/taikangtrack_test/data/recommengwioutalgo_dfApp");
		DataFrame dfAppDefault=getTotalAppDefaultResult(sqlContext);
//		saveAsParquet(dfAppDefault, "/user/tkonline/taikangtrack_test/data/TotalAppDdfaultResult");
	
		DataFrame dfAll=getTotalPvResult(sqlContext);
//		saveAsParquet(dfAll, "/user/tkonline/taikangtrack_test/data/TotalPvResult");
		DataFrame dfAllDefault=getTotalDefaultResult(sqlContext);
		return dfAllDefault;
	}
	/**
	 * 计算默认排名,计算所有用户近一段时间内的产品浏览次数排名
	 * @param sqlContext
	 * @return
	 */
	public DataFrame countTotalProductPv(HiveContext sqlContext){
		String sql="select result.unique_pid,cast(result.pv as string) pv from (SELECT ONEMONTHINFO.unique_pid,SUM(ONEMONTHINFO.VISIT_COUNT) PV FROM"
				+ "(SELECT unique_pid,VISIT_COUNT FROM RBASE_USERBROWSEANDPOLICYINFO WHERE "
				+ "DATEDIFF(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),VISIT_TIME) <= "+days+" AND unique_pid IS NOT NULL "
				+ " ) ONEMONTHINFO "
				+ " GROUP BY ONEMONTHINFO.unique_pid order by PV desc) result";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "UserOneMonthInfoBrowseInfo",DataFrameUtil.CACHETABLE_EAGER);
	}

	/**
	 * 计算默认排名,关联浏览总排名和某个配置表得到该渠道下的id
	 * @param sqlContext
	 * @return
	 */
	public DataFrame getTotalWxProductPv(HiveContext sqlContext){
		String sql="select umi.pv,"+
				"wxp.lrtid as unique_pid,"+
				"wxp.caseid,"+
				"wxp.imageurl,"+
				"wxp.productname,"+
				"wxp.desc1,"+
				"wxp.desc2,"+
				"wxp.desc3,"+
				"wxp.prise, "+
				"wxp.cornerPic "+
				"from UserOneMonthInfoBrowseInfo umi inner join "+bhSchema+"recommend_wxproduct wxp on " +
				"umi.unique_pid=wxp.lrtid order by cast(umi.pv as double) desc limit 5";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "TotalSomeProductInfo");
	}

	/**
	 * 计算默认排名,把其它字段拼接成一个字段
	 * @param sqlContext
	 * @param df
	 * @return
	 */
	public DataFrame getTotalWxContactProductInfo(HiveContext sqlContext){
		String sql="select " +
				"concat('{',concat_ws(',',concat_ws(':','\"productID\"',concat('\"',unique_pid,'\"'))," +
				"concat_ws(':','\"caseid\"',concat('\"',caseid,'\"'))," +
				"concat_ws(':','\"imageUrl\"',concat('\"',imageurl,'\"'))," +
				"concat_ws(':','\"productName\"',concat('\"',productname,'\"'))," +
				"concat_ws(':','\"desc1\"',concat('\"',desc1,'\"'))," +
				"concat_ws(':','\"desc2\"',concat('\"',desc2,'\"'))," +
				"concat_ws(':','\"desc3\"',concat('\"',desc3,'\"'))," +
				"concat_ws(':','\"prise\"',concat('\"',prise,'\"'))," +
				"concat_ws(':','\"cornerPic\"',concat('\"',cornerPic,'\"'))" +
				"),'}') as pinfo" +
				" from TotalSomeProductInfo";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "TotalContactProductInfo");
	}

	public DataFrame getTotalProductResult(HiveContext sqlContext){
		String sql="select concat('[',concat_ws(',',collect_set(sc.pinfo)),']') as total_wx_info " +
				"from  TotalContactProductInfo sc ";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "TotalProductResult",DataFrameUtil.CACHETABLE_EAGER);
	}
	/**
	 * 计算默认排名,关联浏览总排名和wap配置表
	 * @param sqlContext
	 * @return
	 */
	public DataFrame getTotalWapProductPv(HiveContext sqlContext){
		String sql="select umi.pv,"+
				"wap.unique_pid,"+
				"wap.lrtid,"+
				"wap.imageurl,"+
				"wap.productname,"+
				"wap.desc1,"+
				"wap.desc2,"+
				"wap.desc3,"+
				"wap.productURL "+
				"from UserOneMonthInfoBrowseInfo umi inner join "+bhSchema+"recommend_wapproduct wap on " +
				"umi.unique_pid=wap.unique_pid order by cast(umi.pv as double) desc limit 5";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "TotalWapProductInfo");
	}
	/**
	 * 计算默认排名,把其它字段拼接成一个字段 wap
	 * @param sqlContext
	 * @param df
	 * @return
	 */
	public DataFrame getTotalWapContactProductInfo(HiveContext sqlContext){
		String sql="select " +
				"concat('{',concat_ws(',',concat_ws(':','\"combocode\"',concat('\"',unique_pid,'\"'))," +
				"concat_ws(':','\"lrt_id\"',concat('\"',lrtid,'\"'))," +
				"concat_ws(':','\"imageUrl\"',concat('\"',imageurl,'\"'))," +
				"concat_ws(':','\"productName\"',concat('\"',productname,'\"'))," +
				"concat_ws(':','\"desc1\"',concat('\"',desc1,'\"'))," +
				"concat_ws(':','\"desc2\"',concat('\"',desc2,'\"'))," +
				"concat_ws(':','\"desc3\"',concat('\"',desc3,'\"'))," +
				"concat_ws(':','\"productURL\"',concat('\"',productURL,'\"'))" +
				"),'}') as pinfo" +
				" from TotalWapProductInfo";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "TotalWapContactProductInfo");
	}
	public DataFrame getTotalWapProductResult(HiveContext sqlContext){
		String sql="select concat('[',concat_ws(',',collect_set(sc.pinfo)),']') as total_wap_info " +
				"from  TotalWapContactProductInfo sc ";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "TotalWapProductResult");
	}
	
	/**
	 * 计算默认排名,关联浏览总排名和某个配置表得到该渠道下的id
	 * @param sqlContext
	 * @return
	 */
	public DataFrame getTotalPcProduct(HiveContext sqlContext){
		String sql="  select t.product_id, t.lrt_id, t.productName, t.pc_short_url, "
				+ "   t.imageurl,t.price_unit,t.min_price "
				+"  from (select pf.product_id, "
				+"               pc.code as lrt_id, "
				+"               pc.name as productName, "
				+"               pc.pc_short_url,pc.price_unit,pc.min_price, "
				+"               concat(pf.path, pf.origional_name) as imageurl "
				+"          from  "+bhSchema+"product_cms pc "
				+"         inner join "+bhSchema+"product_file pf "
				+"            on pf.product_id = pc.id "
				+"           and pf.site_id = '1' "
				+"           and pf.key_flag = '300' "
				+"           and pf.is_delete = 'false') t "
				+" inner join "+bhSchema+"product_main pm "
				+"    on pm.product_id = t.product_id "
				+"   and pm.status = '1' and pm.product_id <> '10' ";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "pcConfigTable",DataFrameUtil.CACHETABLE_EAGER);
	}
	
	/**
	 * 计算默认排名,关联浏览总排名和某个配置表得到该渠道下的id
	 * @param sqlContext
	 * @return
	 */
	public DataFrame getTotalPcProductPv(HiveContext sqlContext){
		String sql="select umi.pv,"+
				"pc.lrt_id,"+
				"pc.product_id,"+
				"pc.productname,"+
				"pc.pc_short_url,"+
				"pc.imageurl,pc.price_unit,pc.min_price "+
				"from UserOneMonthInfoBrowseInfo umi inner join pcConfigTable pc on " +
				"umi.unique_pid=pc.lrt_id order by cast(umi.pv as double) desc limit 5";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "TotalPcProductInfo");
	}

	/**
	 * 计算默认排名,把其它字段拼接成一个字段
	 * @param sqlContext
	 * @param df
	 * @return
	 */
	public DataFrame getTotalPcContactProductInfo(HiveContext sqlContext){
		String sql="select " +
				"concat('{',concat_ws(',',concat_ws(':','\"lrt_id\"',concat('\"',lrt_id,'\"'))," +
				"concat_ws(':','\"product_id\"',concat('\"',product_id,'\"'))," +
				"concat_ws(':','\"productName\"',concat('\"',productname,'\"'))," +
				"concat_ws(':','\"pc_short_url\"',concat('\"',pc_short_url,'\"'))," +
				"concat_ws(':','\"imageurl\"',concat('\"',imageurl,'\"')), " +
				"concat_ws(':','\"price_unit\"',concat('\"',price_unit,'\"')), " +
				"concat_ws(':','\"min_price\"',concat('\"',min_price,'\"')) " +
				"),'}') as pinfo" +
				" from TotalPcProductInfo";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "TotalPcContactProductInfo");
	}
	
	public DataFrame getTotalPcProductResult(HiveContext sqlContext){
		String sql="select concat('[',concat_ws(',',collect_set(sc.pinfo)),']') as total_pc_info " +
				"from  TotalPcContactProductInfo sc ";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "TotalPcProductResult");
	}	
	
	
	/**
	 * 计算默认排名,关联浏览总排名和app配置表
	 * @param sqlContext
	 * @return
	 */
	public DataFrame getTotalAppProductPv(HiveContext sqlContext){
		String sql="select distinct umi.pv,"+
				"app.name,"+
				"app.brief,"+
				"app.desc,"+
				"app.imageURL,"+
				"app.product_id,"+
				"app.webURL,app.list_brief "+
				"from UserOneMonthInfoBrowseInfo umi inner join "+bhSchema+"recommend_appproduct app on " +
				"umi.unique_pid=app.product_id order by cast(umi.pv as double) desc limit 5";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "TotalAppProductInfo");
	}
	

	/**
	 * 计算默认排名,把其它字段拼接成一个字段
	 * @param sqlContext
	 * @param df
	 * @return
	 */
	public DataFrame getTotalAppContactProductInfo(HiveContext sqlContext){
		
		String sql="select " +
				   "concat('{',concat_ws(',', " +
				     "concat_ws(':','\"product_id\"',concat('\"',product_id,'\"'))," +
					 "concat_ws(':','\"brief\"',concat('\"',brief,'\"'))," +
					 "concat_ws(':','\"desc\"',concat('\"',desc,'\"'))," +
					 "concat_ws(':','\"imgUrl\"',concat('\"',imageURL,'\"'))," +
					 "concat_ws(':','\"intent\"',concat('{',concat_ws(','," +
					 "    concat_ws(':','\"action\"',concat('\"','openWebPage','\"'))," +
					 "    concat_ws(':','\"loginCheck\"',concat('\"','0','\"'))," +
					 "    concat_ws(':','\"webUrl\"',concat('\"',webUrl,'\"')) ),'}'))," +
					 "concat_ws(':','\"name\"',concat('\"',name,'\"'))," +
					 "concat_ws(':','\"list_brief\"',list_brief)" +
					"),'}') as pinfo" +
					" from TotalAppProductInfo";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "TotalAppContactProductInfo");
	}
	
	public DataFrame getTotalAppProductResult(HiveContext sqlContext){

		String sql="select concat('[',concat_ws(',',collect_set(sc.pinfo)),']') as total_app_info " +
		"from  TotalAppContactProductInfo sc ";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "TotalAppProductResult");
	}
	
	public DataFrame getTotalAppDefaultResult(HiveContext sqlContext){

	  String sql=  "select concat('{',concat_ws(',', " +
			      "concat_ws(':','\"intent\"',concat('{',concat_ws(','," +
				  "    concat_ws(':','\"action\"',concat('\"','backToMain','\"'))," +
				  "    concat_ws(':','\"index\"',concat('\"','1','\"'))," +
				  "    concat_ws(':','\"insurance\"','4')," +
				  "    concat_ws(':','\"loginCheck\"',concat('\"','0','\"'))),'}'))," +
				  "concat_ws(':','\"products\"',total_app_info )," +
				  "concat_ws(':','\"title\"',concat('\"','查看全部产品','\"'))," +
				  "concat_ws(':','\"type\"',concat('\"','16','\"'))" +
				 "),'}') as total_app_info " +
				 " from TotalAppProductResult ";
	  
		return DataFrameUtil.getDataFrame(sqlContext, sql, "TotalAppDefaultResult");
	}
	
	
	public DataFrame getTotalPvResult(HiveContext sqlContext){
//		String sql="select (select total_wx_info from TotalProductResult limit 1) as total_wx_info," +
//				"(select total_wap_info from TotalWapProductResult limit 1) as total_wap_info" +
//				" from TotalWapProductResult limit 1" ;
		String sql="select a.total_wx_info,b.total_wap_info,c.total_pc_info ,d.total_app_info from " +
				"(select '1' ids,total_wx_info from TotalProductResult limit 1) a inner join " +
				"(select '1' ids,total_wap_info from TotalWapProductResult limit 1) b inner join " +
				"(select '1' ids,total_pc_info from TotalPcProductResult limit 1) c inner join " +
				"(select '1' ids,total_app_info from TotalAppProductResult limit 1) d " +
				" on a.ids=b.ids and b.ids=c.ids and c.ids=d.ids " ;
		return DataFrameUtil.getDataFrame(sqlContext, sql, "TotalPvResult");
	}

	public DataFrame getTotalDefaultResult(HiveContext sqlContext){
		
		String sql="select a.total_wx_info,b.total_wap_info,c.total_pc_info ,d.total_app_info from " +
				"(select '1' ids,total_wx_info from TotalProductResult limit 1) a inner join " +
				"(select '1' ids,total_wap_info from TotalWapProductResult limit 1) b inner join " +
				"(select '1' ids,total_pc_info from TotalPcProductResult limit 1) c inner join " +
				"(select '1' ids,total_app_info from TotalAppDefaultResult limit 1) d " +
				" on a.ids=b.ids and b.ids=c.ids and c.ids=d.ids " ;
		
		return DataFrameUtil.getDataFrame(sqlContext, sql, "TotalPvDefaultResult");
	}

	/**
	 * 获得推荐结果
	 */
	public DataFrame getRecommendResult(HiveContext sqlContext){
//		App.loadParquet(sqlContext, "/user/tkonline/taikangtrack_test/data/recommondwithoutalgo-20170522", "RBASE_USERBROWSEANDPOLICYINFO", false);
//		App.loadParquet(sqlContext, "/user/tkonline/taikangtrack_test/data/FactHealthcreditscoreresult", "FactHealthcreditscoreResultUserIdNotNullDF", false);
		DataFrame dfgroupRecomend=getUserBrowseAndPolicyInfoGroup(sqlContext);
//		saveAsParquet(dfgroupRecomend, "/user/tkonline/taikangtrack_test/data/recommengwioutalgo_dfgroupRecomend");
		
		DataFrame df=getEachUserBrowseInfo(sqlContext);
//		saveAsParquet(df, "/user/tkonline/taikangtrack/data/recommengwioutalgo_dflimitscore");
		
		DataFrame dfbasescore=analyseScore(df, sqlContext);
//		saveAsParquet(dfbasescore, "/user/tkonline/taikangtrack/data/recommengwioutalgo_basescore");
        
		DataFrame wxPinfo=getResultWxProductInfo(sqlContext);
//		saveAsParquet(wxPinfo, "/user/tkonline/taikangtrack/data/recommengwioutalgo_wxPinfo");
		getContactProductInfo(sqlContext);
		DataFrame contactPinfoGroup=getContactProductInfoGroup(sqlContext);
//		saveAsParquet(contactPinfoGroup, "/user/tkonline/taikangtrack_test/data/recommengwioutalgo_contactPinfoGroup");

		//wap
		getResultWapProductInfo(sqlContext);
		getContactWapProductInfo(sqlContext);
		DataFrame df2=getContactWapProductInfoGroup(sqlContext);
//		saveAsParquet(df2, "/user/tkonline/taikangtrack_test/data/recommengwioutalgo_ContactWapProductInfoGroup");
        
		//pc   
		getResultPcProductInfo(sqlContext);
		getContactPcProductInfo(sqlContext);
		DataFrame df3=getContactPcProductInfoGroup(sqlContext);
//		saveAsParquet(df3, "/user/tkonline/taikangtrack_test/data/recommengwioutalgo_ContactPcProductInfoGroup");
		
		//app
		getResultAppProductInfo(sqlContext);
		getContactAppProductInfo(sqlContext);
		DataFrame df3App=getContactAppProductInfoGroup(sqlContext);
//		saveAsParquet(df3App, "/user/tkonline/taikangtrack_test/data/recommengwioutalgo_ContactAppProductInfoGroup");
		
		DataFrame dfall=getAllInfoGroup(sqlContext);
//		saveAsParquet(dfall, "/user/tkonline/taikangtrack_test/data/recommengwioutalgo_AllInfoGroup");
		DataFrame dfsss33=getDistinctUserIdFromGroup(sqlContext);
//		saveAsParquet(dfsss33, "/user/tkonline/taikangtrack_test/data/getDistinctUserIdFromGroup");
		DataFrame dfsss22=getAllFromHealthResult(sqlContext);
//		saveAsParquet(dfsss22, "/user/tkonline/taikangtrack_test/data/getAllFromHealthResult");
		DataFrame dfsss=getMemberIdAndUIdFromHealthResult(sqlContext);
//		saveAsParquet(dfsss, "/user/tkonline/taikangtrack_test/data/recommengwioutalgo_MemberIdAndUIdFromHealthResult");
		
//		App.loadParquet(sqlContext, "/user/tkonline/taikangtrack_test/data/recommengwioutalgo_MemberIdAndUIdFromHealthResult", "MemberIdAndUIdFromHealthResult", false);
//		App.loadParquet(sqlContext, "/user/tkonline/taikangtrack_test/data/TotalPvResult", "TotalPvResult", false);
		DataFrame df0=deleteVaildId(sqlContext);
//		saveAsParquet(df0, "/user/tkonline/taikangtrack_test/data/recommengwioutalgo_deleteVaildId");
		
		DataFrame df000=supplementPinfo(sqlContext,df0);
//		saveAsParquet(df000, "/user/tkonline/taikangtrack_test/data/supplementPinfo");
		
		DataFrame dfwap=fliterWapInfo(sqlContext);
		saveAsParquet(dfwap,wapResultPath);

		DataFrame dff=fliterWxInfo(sqlContext);
//		DataFrame finalResult=supplementPinfo(sqlContext,dff);

		DataFrame dfPc=fliterPcInfo(sqlContext);
	    saveAsParquet(dfPc,pcResultPath);
	   
		DataFrame dfapp=fliterAppInfo(sqlContext);
	    saveAsParquet(dfapp,appResultPath);
	    
		 return dff;
	}

	private DataFrame getUserBrowseAndPolicyInfoGroup(HiveContext sqlContext){
//		String sql="select terminal_id,user_id,lrt_id,sum(visit_duration) visit_duration,sum(visit_count) visit_count from UserBrowseAndPolicyInfo ubapi where "
//				+ "ubapi.isvalid='false' and ubapi.isbuy='false' group by terminal_id,user_id,lrt_id";
		
		String sql="SELECT '' AS TERMINAL_ID,USER_ID,unique_pid,lrt_id,SUM(VISIT_DURATION) VISIT_DURATION,SUM(VISIT_COUNT) VISIT_COUNT FROM RBASE_USERBROWSEANDPOLICYINFO UBAPI WHERE  " +
				" UBAPI.ISVALID='false' AND UBAPI.ISBUY='false' AND USER_ID IS NOT NULL AND USER_ID <>'' "
				+ "AND LRT_ID IS NOT NULL AND LRT_ID <>'' GROUP BY USER_ID,unique_pid,lrt_id " +
				"UNION ALL " + 
				"SELECT  TERMINAL_ID,'' AS USER_ID,unique_pid,lrt_id,SUM(VISIT_DURATION) VISIT_DURATION,SUM(VISIT_COUNT) VISIT_COUNT FROM RBASE_USERBROWSEANDPOLICYINFO UBAPI WHERE  " +
				" UBAPI.ISVALID='false' AND UBAPI.ISBUY='false' AND ( USER_ID IS  NULL OR USER_ID ='' ) AND TERMINAL_ID<>'' AND TERMINAL_ID IS NOT NULL "
				+ "AND LRT_ID IS NOT NULL AND LRT_ID <>'' GROUP BY TERMINAL_ID,unique_pid,lrt_id";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "groupUserBrowseInfo");
	}
	
	/**
	 * 
	 * @param sqlContext
	 * @return
	 */
	
	public DataFrame getEachUserBrowseInfo(HiveContext sqlContext){
		String sql="select ubapi.terminal_id,ubapi.user_id,ubapi.lrt_id,ubapi.unique_pid,"
				+"CASE WHEN cast(cast(ubapi.visit_duration as bigint) / 1000 as bigint)  > 600 THEN '600' "
				+ "ELSE cast(cast(ubapi.visit_duration as bigint) / 1000 as string) END AS duration, "
				+"CASE WHEN cast(cast(ubapi.visit_count as bigint) as bigint)  > 12 THEN '12' "
				+ "ELSE cast(cast(ubapi.visit_count as bigint) as string) END AS visit_count "
				+ " from "
				+ "groupUserBrowseInfo ubapi";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "eachUserBrowseInfo");
	}
	public DataFrame analyseScore(DataFrame df,HiveContext hiveContext) {
		 JavaRDD<Row> jRDD= df.select("terminal_id","user_id","unique_pid","lrt_id","duration","visit_count").rdd().toJavaRDD();
		 
		 JavaRDD<RecommendWithoutAlgoScore> resultRdd=jRDD.map(new Function<Row, RecommendWithoutAlgoScore>() {

			public RecommendWithoutAlgoScore call(Row v1) throws Exception {
		
				//计算每条数据的评分
				String terminal_id =v1.getString(0);
				String user_id =v1.getString(1);
				String unique_pid =v1.getString(2);
				String lrt_id =v1.getString(3);

				String visit_duration =v1.getString(4);
				String visit_count =v1.getString(5);
				double duration_D=Double.valueOf(visit_duration);
				double count_D=Double.valueOf(visit_count);
				/*
				 * duration_D 0-600  
				 * count_D   0-12
				 * 将它们缩放到  0-12
				 */
				double x1=duration_D/50;
				double x2=count_D;
				/*
				 * 计算 得分。百分制。  比重 7：3
				 * 分 0-100
				 */
				double percent_d=Double.valueOf(percent)/100;
				
//				double score=100*(Sigmoid(x1)*(1-percent_d)+Sigmoid(x2)*percent_d);
				double score=100*((x1/12)*(1-percent_d)+(x2/12)*percent_d);
				
				RecommendWithoutAlgoScore algoScore=new RecommendWithoutAlgoScore();
				algoScore.setTerminal_id(terminal_id);
				algoScore.setUser_id(user_id);
				algoScore.setUnique_pid(unique_pid);
				algoScore.setLrt_id(lrt_id);
				algoScore.setScore(String.valueOf(score));

				
				return algoScore;
			}
		 });
		 //注册临时表
	      DataFrame scoreDf=hiveContext.createDataFrame(resultRdd, RecommendWithoutAlgoScore.class);

	      scoreDf.registerTempTable("temp_recommengwioutalgo_score");
		  hiveContext.cacheTable("temp_recommengwioutalgo_score");
		 return scoreDf;
	}
	
//	public DataFrame produceRecommendResult(HiveContext sqlContext) {
//
//		String sql="select sc.user_id,'' as terminal_id,concat_ws(',',collect_set(sc.p_score)) as result from ( " +
//				"select user_id,terminal_id, concat_ws(':',lrt_id,score) as p_score from temp_recommengwioutalgo_score " +
//				") sc  where sc.user_id is not null and sc.user_id<>'' group by sc.user_id " +
//				"union all " +
//				"select '' as user_id,sc.terminal_id,concat_ws(',',collect_set(sc.p_score)) as result from ( " +
//				"select user_id,terminal_id, concat_ws(':',lrt_id,score) as p_score from temp_recommengwioutalgo_score " +
//				") sc  where (sc.user_id is  null or sc.user_id='') group by sc.terminal_id ";
//		 return DataFrameUtil.getDataFrame(sqlContext, sql, "RecommendWithoutAlgoResult");
//
//	}
//	public DataFrame produceRecommendResultF(HiveContext sqlContext) {
//		String sql="select r.*,h.member_id,h.open_id from temp_recommengwioutalgo_score r left join FactHealthcreditscoreResultUserIdNotNullDF h on  h.user_id=r.user_id";
//		 return DataFrameUtil.getDataFrame(sqlContext, sql, "RecommendWithoutAlgoResultF");
//
//	}

	/**
	 * 得到所有组合关系
	 * @param sqlContext
	 * @return
	 */
	public DataFrame getOrginalUserTerminalMap(HiveContext sqlContext) {
		String sql="select  sc.user_id,'' as terminal_id from " +
				"temp_recommengwioutalgo_score sc where sc.user_id is not null and sc.user_id<>'' " +
				"group by sc.user_id " +
				" union all " +
				"select  '' as user_id,sc.terminal_id from " +
				"temp_recommengwioutalgo_score sc  where (sc.user_id is  null or sc.user_id='') " +
				"group by sc.terminal_id";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "OrginalUserTerminalMap");

	}
	/**
	 * 关联总表和wap配置表得到该渠道下的id
	 * @param sqlContext
	 * @return
	 */
	public DataFrame  getResultWapProductInfo(HiveContext sqlContext){
		String sql="select raf.user_id, " +
				"raf.terminal_id,"+
				"raf.score,"+
				"raf.lrt_id,"+
				"wap.unique_pid,"+
				"wap.imageurl,"+
				"wap.productname,"+
				"wap.desc1,"+
				"wap.desc2,"+
				"wap.desc3,"+
				"wap.productURL "+
				"from temp_recommengwioutalgo_score raf inner join "+bhSchema+"recommend_wapproduct wap on " +
				"raf.unique_pid=wap.unique_pid order by cast(raf.score as double) desc";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "ResultWapProductInfo");
	}
	/**
	 * 把其它字段拼接成一个字段 wap
	 * @param sqlContext
	 * @param df
	 * @return
	 */
	public DataFrame getContactWapProductInfo(HiveContext sqlContext){
		String sql="select user_id," +
				"terminal_id," +
				"concat('{',concat_ws(',',concat_ws(':','\"combocode\"',concat('\"',unique_pid,'\"'))," +
				"concat_ws(':','\"lrt_id\"',concat('\"',lrt_id,'\"'))," +
				"concat_ws(':','\"imageUrl\"',concat('\"',imageurl,'\"'))," +
				"concat_ws(':','\"productName\"',concat('\"',productname,'\"'))," +
				"concat_ws(':','\"desc1\"',concat('\"',desc1,'\"'))," +
				"concat_ws(':','\"desc2\"',concat('\"',desc2,'\"'))," +
				"concat_ws(':','\"desc3\"',concat('\"',desc3,'\"'))," +
				"concat_ws(':','\"productURL\"',concat('\"',productURL,'\"'))" +
				"),'}') as pinfo" +
				" from ResultWapProductInfo";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "ContactWapProductInfo");
	}
	/**
	 * 拼接产品信息后以用户聚合 微信
	 * @param sqlContext
	 * @param df
	 * @return
	 */
	public DataFrame getContactWapProductInfoGroup(HiveContext sqlContext){
		String sql="select sc.user_id,'' as terminal_id,concat('[',concat_ws(',',collect_set(sc.pinfo)),']') as wapinfo from " +
				"ContactWapProductInfo sc  where sc.user_id is not null and sc.user_id<>'' group by sc.user_id " +
				"union all " +
				"select '' as user_id,sc.terminal_id,concat('[',concat_ws(',',collect_set(sc.pinfo)),']') as wapinfo from " +
				"ContactWapProductInfo sc  where (sc.user_id is  null or sc.user_id='') group by sc.terminal_id ";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "ContactWapProductInfoGroup");
	}
   
	
	/**
	 * 关联总表和某个配置表得到该渠道下的id
	 * @param sqlContext
	 * @return
	 */
	public DataFrame getResultPcProductInfo(HiveContext sqlContext){
		String sql="select  raf.user_id, " +
				"raf.terminal_id,"+
				"raf.score,"+
				"pc.lrt_id,"+ 
				"pc.product_id,"+
				"pc.productname,"+
				"pc.pc_short_url,"+
				"pc.imageurl,pc.price_unit,pc.min_price "+
				"from temp_recommengwioutalgo_score raf,pcConfigTable pc where " +
				"raf.lrt_id=pc.lrt_id order by cast(raf.score as double) desc";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "ResultPcProductInfo");
	}

	/**
	 * 把其它字段拼接成一个字段 wechat
	 * @param sqlContext
	 * @param df
	 * @return
	 */
	public DataFrame getContactPcProductInfo(HiveContext sqlContext){
		String sql="select user_id," +
				"terminal_id," +
				"concat('{',concat_ws(',',concat_ws(':','\"lrt_id\"',concat('\"',lrt_id,'\"'))," +
				"concat_ws(':','\"product_id\"',concat('\"',product_id,'\"'))," +
				"concat_ws(':','\"productName\"',concat('\"',productName,'\"'))," +
				"concat_ws(':','\"pc_short_url\"',concat('\"',pc_short_url,'\"'))," +
				"concat_ws(':','\"imageurl\"',concat('\"',imageurl,'\"'))," +
				"concat_ws(':','\"price_unit\"',concat('\"',price_unit,'\"')), " +
				"concat_ws(':','\"min_price\"',concat('\"',min_price,'\"')) " +
				"),'}') as pinfo" +
				" from ResultPcProductInfo";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "ContactPcProductInfo");
	}

	
	/**
	 * 拼接产品信息后以用户聚合 pc
	 * @param sqlContext
	 * @param df
	 * @return
	 */
	public DataFrame getContactPcProductInfoGroup(HiveContext sqlContext){
		String sql="select sc.user_id,'' as terminal_id,concat('[',concat_ws(',',collect_set(sc.pinfo)),']') as pcinfo from " +
				"ContactPcProductInfo sc  where sc.user_id is not null and sc.user_id<>'' group by sc.user_id " +
				"union all " +
				"select '' as user_id,sc.terminal_id,concat('[',concat_ws(',',collect_set(sc.pinfo)),']') as pcinfo from " +
				"ContactPcProductInfo sc  where (sc.user_id is  null or sc.user_id='') group by sc.terminal_id ";
		 return DataFrameUtil.getDataFrame(sqlContext, sql, "ContactPcProductInfoGroup");
	}
	
	/**
	 * 关联总表和app配置表得到该渠道下的id
	 * @param sqlContext
	 * @return
	 */
	public DataFrame  getResultAppProductInfo(HiveContext sqlContext){
		String sql="select raf.user_id, " +
				"raf.terminal_id,"+
				"raf.score,"+
				"app.name,"+
				"app.brief,"+
				"app.imageURL,"+
				"app.product_id,"+
				"app.desc,"+
				"app.webURL,app.list_brief "+
				"from temp_recommengwioutalgo_score raf inner join "+bhSchema+"recommend_appproduct app on " +
				"raf.unique_pid=app.product_id order by cast(raf.score as double) desc";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "ResultAppProductInfo");
	}
	/**
	 * 把其它字段拼接成一个字段 wap
	 * @param sqlContext
	 * @param df
	 * @return
	 */
	public DataFrame getContactAppProductInfo(HiveContext sqlContext){
		
		String sql="select user_id," +
				"terminal_id," +
				"concat('{',concat_ws(',', " +
			     "concat_ws(':','\"product_id\"',concat('\"',product_id,'\"'))," +
				 "concat_ws(':','\"brief\"',concat('\"',brief,'\"'))," +
				 "concat_ws(':','\"desc\"',concat('\"',desc,'\"'))," +
				 "concat_ws(':','\"imgUrl\"',concat('\"',imageURL,'\"'))," +
				 "concat_ws(':','\"intent\"',concat('{',concat_ws(','," +
				 "    concat_ws(':','\"action\"',concat('\"','openWebPage','\"'))," +
				 "    concat_ws(':','\"loginCheck\"',concat('\"','0','\"'))," +
				 "    concat_ws(':','\"webUrl\"',concat('\"',webUrl,'\"')) ),'}'))," +
				 "concat_ws(':','\"name\"',concat('\"',name,'\"'))," +
				 "concat_ws(':','\"list_brief\"',list_brief)" +
				"),'}') as pinfo" +
				" from ResultAppProductInfo";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "ContactAppProductInfo");
	}
	
	
	/**
	 * 拼接产品信息后以用户聚合 微信
	 * @param sqlContext
	 * @param df
	 * @return
	 */
	public DataFrame getContactAppProductInfoGroup(HiveContext sqlContext){
		
		String sql="select sc.user_id,'' as terminal_id,concat('[',concat_ws(',',collect_set(sc.pinfo)),']') as appinfo from " +
				"ContactAppProductInfo sc  where sc.user_id is not null and sc.user_id<>'' group by sc.user_id " +
				"union all " +
				"select '' as user_id,sc.terminal_id,concat('[',concat_ws(',',collect_set(sc.pinfo)),']') as appinfo from " +
				"ContactAppProductInfo sc  where (sc.user_id is  null or sc.user_id='') group by sc.terminal_id ";
		
		return DataFrameUtil.getDataFrame(sqlContext, sql, "ContactAppProductInfoGroup");
	}
	
	/**
	 * 关联总表和某个配置表得到该渠道下的id
	 * @param sqlContext
	 * @return
	 */
	public DataFrame getResultWxProductInfo(HiveContext sqlContext){
		String sql="select  raf.user_id, " +
				"raf.terminal_id,"+
				"raf.score,"+
				"wxp.lrtid,"+
				"wxp.caseid,"+
				"wxp.imageurl,"+
				"wxp.productname,"+
				"wxp.desc1,"+
				"wxp.desc2,"+
				"wxp.desc3,"+
				"wxp.prise, "+
				"wxp.cornerPic "+
				"from temp_recommengwioutalgo_score raf,"+bhSchema+"recommend_wxproduct wxp where " +
				"raf.lrt_id=wxp.lrtid order by cast(raf.score as double) desc";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "ResultWxProductInfo");
	}

	/**
	 * 把其它字段拼接成一个字段 wechat
	 * @param sqlContext
	 * @param df
	 * @return
	 */
	public DataFrame getContactProductInfo(HiveContext sqlContext){
		String sql="select user_id," +
				"terminal_id," +
				"concat('{',concat_ws(',',concat_ws(':','\"productID\"',concat('\"',lrtid,'\"'))," +
				"concat_ws(':','\"caseid\"',concat('\"',caseid,'\"'))," +
				"concat_ws(':','\"imageUrl\"',concat('\"',imageurl,'\"'))," +
				"concat_ws(':','\"productName\"',concat('\"',productname,'\"'))," +
				"concat_ws(':','\"desc1\"',concat('\"',desc1,'\"'))," +
				"concat_ws(':','\"desc2\"',concat('\"',desc2,'\"'))," +
				"concat_ws(':','\"desc3\"',concat('\"',desc3,'\"'))," +
				"concat_ws(':','\"prise\"',concat('\"',prise,'\"'))," +
				"concat_ws(':','\"cornerPic\"',concat('\"',cornerPic,'\"'))" +
				"),'}') as pinfo" +
				" from ResultWxProductInfo";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "ContactProductInfo");
	}

	/**
	 * 拼接产品信息后以用户聚合 微信
	 * @param sqlContext
	 * @param df
	 * @return
	 */
	public DataFrame getContactProductInfoGroup(HiveContext sqlContext){
		String sql="select sc.user_id,'' as terminal_id,concat('[',concat_ws(',',collect_set(sc.pinfo)),']') as wxpinfo from " +
				"ContactProductInfo sc  where sc.user_id is not null and sc.user_id<>'' group by sc.user_id " +
				"union all " +
				"select '' as user_id,sc.terminal_id,concat('[',concat_ws(',',collect_set(sc.pinfo)),']') as wxpinfo from " +
				"ContactProductInfo sc  where (sc.user_id is  null or sc.user_id='') group by sc.terminal_id ";
		 return DataFrameUtil.getDataFrame(sqlContext, sql, "ContactProductInfoGroup");
	}

	/**
	 * full join
	 * @param sqlContext
	 * @return
	 */
	public DataFrame getAllInfoGroup(HiveContext sqlContext){

//		String sql=" select case when a1.user_id is not null then a1.user_id else a2.user_id end as user_id," +
//				"case when a1.terminal_id is not null then a1.terminal_id else a2.terminal_id end as terminal_id," +
//				"a1.wxpinfo,a2.wapinfo as wappinfo,'' as apppinfo,'' as pcpinfo " +
//				"from ContactProductInfoGroup a1 full join ContactWapProductInfoGroup a2 on " +
//				"a1.user_id=a2.user_id or a1.terminal_id=a2.terminal_id";
		/*
		full join 执行不动，改以info_type 区分推荐结果类型，多推一次hbase
		0 wechat
		1 wap
		2 pc
		3 app
		*/
		String sql="select a1.user_id,a1.terminal_id,a1.wxpinfo as pinfo,'0' as info_type from ContactProductInfoGroup a1 " +
				"union all " +
				"select a2.user_id,a2.terminal_id,a2.wapinfo as pinfo,'1' as info_type from ContactWapProductInfoGroup a2 " +
		        "union all " +
		        "select a3.user_id,a3.terminal_id,a3.pcinfo as pinfo,'2' as info_type from ContactPcProductInfoGroup a3 " +
		        "union all " +
		        "select a4.user_id,a4.terminal_id,a4.appinfo as pinfo,'3' as info_type from ContactAppProductInfoGroup a4 ";
//		String sql="select terminal_id,user_id,pinfo from (" +
//				"select terminal_id,user_id,concat('1_',wxpinfo) as pinfo from ContactProductInfoGroup t1 " +
//				"union all " +
//				"select terminal_id,user_id,concat('2_',wapinfo) as pinfo from  ContactWapProductInfoGroup t2 " +
//				")b order by pinfo ";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "AllInfoGroup");
	}

//	public DataFrame getJoin4ProductInfo(HiveContext sqlContext){
//		String sql="select terminal_id,user_id,pinfo[0] as wxpinfo,pinfo[1] wappinfo,'' as apppinfo,'' as pcpinfo from (" +
//				"select terminal_id,user_id,collect_set(pinfo) as pinfo from AllInfoGroup" +
//				" group by terminal_id,user_id " +
//				") t";
//
//		return DataFrameUtil.getDataFrame(sqlContext, sql, "Join4ProductInfo");
//	}

	/**
	 * 关联对应关系表和各端的结果表得到最终 结果
	 * @param sqlContext
	 * @param df
	 * @return
	 */
	public DataFrame getJoinOrginalWx(HiveContext sqlContext){
		String sql="" +
				"select outm.user_id,outm.terminal_id,cpig.wxpinfo from  " +
				"OrginalUserTerminalMap outm,ContactProductInfoGroup cpig  where " +
				"outm.user_id is not null and outm.user_id <> '' and outm.user_id=cpig.user_id " +
				"union all " +
				"select outm.user_id,outm.terminal_id,cpig.wxpinfo from " +
				" OrginalUserTerminalMap outm,ContactProductInfoGroup cpig  where  " +
				"outm.user_id is null or  outm.user_id ='' and outm.terminal_id=cpig.terminal_id ";

		return DataFrameUtil.getDataFrame(sqlContext, sql, "OrginalWx");
	}
	
  public DataFrame getDistinctUserIdFromGroup(HiveContext sqlContext){
		
		String sql="select distinct user_id from AllInfoGroup where user_id is not null and user_id <> '' ";

		return DataFrameUtil.getDataFrame(sqlContext, sql, "DistinctAllGroup");
	}
  
	public DataFrame getAllFromHealthResult(HiveContext sqlContext){
		
		String sql="select  /*+MAPJOIN(r)*/ r.user_id,h.member_id,h.open_id from  DistinctAllGroup r " +
				"inner join FactHealthcreditscoreResultUserIdNotNullDF h on  r.user_id=h.user_id  " ;

		return DataFrameUtil.getDataFrame(sqlContext, sql, "AllFromHealthResult");
	}
	/**
	 * 关联大健康表拿到用户openid 和memberid 得到最终 结果
	 * @param sqlContext
	 * @param df
	 * @return
	 */
	public DataFrame getMemberIdAndUIdFromHealthResult(HiveContext sqlContext){
//		String sql="select /*+MAPJOIN(r)*/ r.user_id,r.terminal_id ,r.wxpinfo,r.wappinfo,r.apppinfo,r.pcpinfo," +
//				"h.member_id,h.open_id from  Join4ProductInfo r " +
//				"left join FactHealthcreditscoreResultUserIdNotNullDF h on  r.user_id=h.user_id where " +
//				"h.user_id is not null and h.user_id <>'' ";
//		String sql="select /*+MAPJOIN(r)*/ r.user_id,r.terminal_id ,r.pinfo,r.info_type," +
//				"h.member_id,h.open_id from  AllInfoGroup r " +
//				"left join FactHealthcreditscoreResultUserIdNotNullDF h on  r.user_id=h.user_id  " ;
//		
		String sql="select r.user_id,r.terminal_id ,r.pinfo,r.info_type," +
				"h.member_id,h.open_id from  AllInfoGroup r " +
				"left join AllFromHealthResult h on  r.user_id=h.user_id  " ;

		return DataFrameUtil.getDataFrame(sqlContext, sql, "MemberIdAndUIdFromHealthResult");
	}

	/**
	 * 删除之前随机生成的无效openId和memberid,删除之前在产品列表前添加的 1_和2_
	 * @param sqlContext
	 * @return
	 */
	public DataFrame deleteVaildId(HiveContext sqlContext){
//		String sql="select xt.terminal_id,xt.user_id,CASE " +
//				"WHEN split(xt.open_id,'_ID')[0]='OPEN' THEN  " +
//				"''" +
//				"ELSE " +
//				"xt.open_id " +
//				"END AS open_id," +
//				"CASE " +
//				"WHEN split(xt.member_id,'_ID')[0]='MEMBER' THEN " +
//				"'' " +
//				"ELSE  " +
//				"xt.member_id " +
//				"END AS member_id," +
//				"substring(xt.wxpinfo,3) as wxpinfo," +
//				"xt.pcpinfo," +
//				"substring(xt.wappinfo,3) as wappinfo," +
//				"xt.apppinfo " +
//				"from MemberIdAndUIdFromHealthResult xt ";
		String sql="select xt.terminal_id,xt.user_id,CASE " +
				"WHEN split(xt.open_id,'_ID')[0]='OPEN' THEN  " +
				"''" +
				"ELSE " +
				"xt.open_id " +
				"END AS open_id," +
				"CASE " +
				"WHEN split(xt.member_id,'_ID')[0]='MEMBER' THEN " +
				"'' " +
				"ELSE  " +
				"xt.member_id " +
				"END AS member_id,xt.pinfo,xt.info_type " +
				"from MemberIdAndUIdFromHealthResult xt ";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "deleteVaildIdDf");
	}

	/**
	 * 筛选出微信数据，
	 * @param sqlContext
	 * @return
	 */
	public DataFrame fliterWxInfo(HiveContext sqlContext){
		String sql="select user_id,member_id,open_id,terminal_id,'' as pcpinfo,'' as wappinfo," +
				" '' as apppinfo,pinfo as wxpinfo from " +
				"RecommendWithoutAlgoMidResult df where info_type='0' ";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "fliterWxInfo");
	}
	/**
	 * 筛选出wap数据，独立推送hbase
	 * @param sqlContext
	 * @return
	 */
	public DataFrame fliterPcInfo(HiveContext sqlContext){
		String sql="select user_id,member_id,open_id,terminal_id,pinfo as pcpinfo from " +
				"RecommendWithoutAlgoMidResult df where info_type='2' ";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "fliterPcInfo");
	}
	
	/**
	 * 筛选出wap数据，独立推送hbase
	 * @param sqlContext
	 * @return
	 */
    
	public DataFrame fliterAppInfo(HiveContext sqlContext){
		String sql="select user_id,member_id, open_id,terminal_id ," +
		 "concat('{',concat_ws(',', " +
	     "concat_ws(':','\"intent\"',concat('{',concat_ws(','," +
		 "    concat_ws(':','\"action\"',concat('\"','backToMain','\"'))," +
		 "    concat_ws(':','\"index\"',concat('\"','1','\"'))," +
		 "    concat_ws(':','\"insurance\"','4')," +
		 "    concat_ws(':','\"loginCheck\"',concat('\"','0','\"'))),'}'))," +
		 "concat_ws(':','\"products\"',pinfo )," +
		 "concat_ws(':','\"title\"',concat('\"','查看全部产品','\"'))," +
		 "concat_ws(':','\"type\"',concat('\"','16','\"'))" +
		"),'}') as appinfo " +
		" from RecommendWithoutAlgoMidResult  where info_type='3' ";
		
		
		return DataFrameUtil.getDataFrame(sqlContext, sql, "fliterAppAllInfo");
	}
	
	/**
	 * 筛选出pc数据，独立推送hbase
	 * @param sqlContext
	 * @return
	 */
	public DataFrame fliterWapInfo(HiveContext sqlContext){
		String sql="select user_id,member_id,open_id,terminal_id,pinfo as wappinfo from " +
				"RecommendWithoutAlgoMidResult df where info_type='1' ";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "fliterWapInfo");
	}
	
	
	public DataFrame supplementPinfo(HiveContext sqlContext,DataFrame df0){

		String sql="select total_wx_info,total_wap_info,total_pc_info,total_app_info from TotalPvResult";


		DataFrame df = DataFrameUtil.getDataFrame(sqlContext, sql, "TotalProductResult1");
		String result=df.select("total_wx_info","total_wap_info","total_pc_info","total_app_info").first().getString(0);
		String resultWap=df.select("total_wx_info","total_wap_info","total_pc_info","total_app_info").first().getString(1);
		String resultpc=df.select("total_wx_info","total_wap_info","total_pc_info","total_app_info").first().getString(2);
        String resultApp=df.select("total_wx_info","total_wap_info","total_pc_info","total_app_info").first().getString(3);
		final String  totalResult=result;
		final String  totalWapResult=resultWap;
		final String  totalPcResult=resultpc;
		final String totalAppResult=resultApp;
//terminal_id,user_id,open_id,member_id,pinfo,info_type
		JavaRDD<Row> rddRes=df0.select("user_id", "member_id", "open_id", "terminal_id", "pinfo", "info_type")
				.rdd().toJavaRDD();
		JavaRDD<RecommendWithoutAlgoMidBean> rddResult=rddRes.map(new Function<Row, RecommendWithoutAlgoMidBean>() {
			private static final long serialVersionUID = -4846114781244564430L;

			@Override
			public RecommendWithoutAlgoMidBean call(Row v1) throws Exception {
				String user_id = v1.getString(0);
				String member_id = v1.getString(1);
				String open_id = v1.getString(2);
				String terminal_id = v1.getString(3);
				String pinfo = v1.getString(4);
				String info_type = v1.getString(5);

				String newPinfo="";
				//计算把默认产品补充后新的结果
				if("0".equals(info_type)){//微信端
					String wxpinfo=pinfo;
					newPinfo=ContactProductInfo(wxpinfo,totalResult,"productID");
				}else if("1".equals(info_type)){ //wap 端
					String wappinfo=pinfo;
					newPinfo=ContactProductInfo(wappinfo,totalWapResult,"combocode");
				}else if("2".equals(info_type)){ //pc 端
					String pcpinfo=pinfo;
					newPinfo=ContactProductInfo(pcpinfo,totalPcResult,"lrt_id");
				}else if("3".equals(info_type)){ //app 端
					String appinfo=pinfo;
					newPinfo=ContactProductInfo(appinfo,totalAppResult,"product_id");
				}
				RecommendWithoutAlgoMidBean rwar = new RecommendWithoutAlgoMidBean();
				rwar.setUser_id(user_id);
				rwar.setMember_id(member_id);
				rwar.setOpen_id(open_id);
				rwar.setTerminal_id(terminal_id);
				rwar.setInfo_type(info_type);
				rwar.setPinfo(newPinfo);
				return rwar;
			}
		});

		DataFrame resultDf=sqlContext.createDataFrame(rddResult, RecommendWithoutAlgoMidBean.class);
		resultDf.registerTempTable("RecommendWithoutAlgoMidResult");
		return resultDf;
	}

	/**
	 * 补充人工干涉结果到推荐结果里,放在前面
	 * @param sqlContext
	 * @return
	 */
	public DataFrame supplementForceInfo(HiveContext sqlContext,DataFrame df0,String forceResult){
//		String sql="select /*+MAPJOIN(force)*/  res.user_id," +
//				"res.member_id," +
//				"res.open_id," +
//				"res.terminal_id," +
//				"res.pcpinfo," +
//				"res.wappinfo," +
//				"res.apppinfo," +
//				"contactproductinfo(force.forceproduct,res.wxpinfo) as wxpinfo " +
//				"from RecommendWithoutAlgoResult res ,"+bhSchema+"recommend_force force";
//		return DataFrameUtil.getDataFrame(sqlContext, sql, "RecomendSupplementForceInfo");

	   final String  orginalResult=forceResult;


		JavaRDD<Row> rddRes=df0.select("user_id", "member_id", "open_id", "terminal_id", "pcpinfo", "wappinfo", "apppinfo", "wxpinfo")
				.rdd().toJavaRDD();
		JavaRDD<RecomendWithoutAlgoResultBean> rddResult=rddRes.map(new Function<Row, RecomendWithoutAlgoResultBean>() {
			private static final long serialVersionUID = -4846114781244564430L;

			@Override
			public RecomendWithoutAlgoResultBean call(Row v1) throws Exception {
				String user_id = v1.getString(0);
				String member_id = v1.getString(1);
				String open_id = v1.getString(2);
				String terminal_id = v1.getString(3);
				String pcpinfo = v1.getString(4);
				String wappinfo = v1.getString(5);
				String apppinfo = v1.getString(6);
				String wxpinfo = v1.getString(7);
				//计算把强推产品补充后新的结果
				String newWxpinfo = ContactProductInfo(orginalResult, wxpinfo,"productID");

				RecomendWithoutAlgoResultBean rwar = new RecomendWithoutAlgoResultBean(user_id, member_id, open_id, terminal_id,
						pcpinfo, wappinfo, apppinfo, newWxpinfo);
				return rwar;
			}
		});

		DataFrame resultDf=sqlContext.createDataFrame(rddResult, RecomendWithoutAlgoResultBean.class);
		resultDf.registerTempTable("RecomendSupplementForceInfo");
		return resultDf;
	}


	private String ContactProductInfo(String str1,String str2,String paramName){
		String result ="";
		JSONArray jarray= JSONArray.fromObject(str1); //浏览产品
		JSONArray jarray2= JSONArray.fromObject(str2);//默认产品
		int defaultRecomendNum=Integer.valueOf(defaultReNum);
		if(jarray.size()>defaultRecomendNum){
			int discardNum=jarray.size()-defaultRecomendNum;
			for(int i=0;i<discardNum;i++){
				jarray.discard(jarray.size()-1);
			}
			result=jarray.toString();
		}else if(jarray.size()==defaultRecomendNum){
			result=str1;
		}else{
			//要补充的数量
			int suppleNum=defaultRecomendNum-jarray.size();
			//记录现在已经有的产品,补充时排除
			List<String> list = new ArrayList<String>();
			for(int k=0;k<jarray.size();k++){
				JSONObject oneJson1=jarray.getJSONObject(k);
//				String orginalId=oneJson1.getString("productID");
				String orginalId=oneJson1.getString(paramName);
				list.add(orginalId);
			}
			for(int i=0;i<suppleNum && i<jarray2.size();i++){
				JSONObject obj2Add=jarray2.getJSONObject(i);
//				String suppleProductId=obj2Add.getString("productID");
				String suppleProductId=obj2Add.getString(paramName);
				if(list.contains(suppleProductId)){
					//跳过当前重复的,要补充的量往后推一
					suppleNum=suppleNum+1;
				}else{
					jarray.add(obj2Add);
				}
			}
			result=jarray.toString();
		}
		return result;
	}


	
	public void saveAsParquet(DataFrame df, String path) {
        TK_DataFormatConvertUtil.deletePath(path);
        df.saveAsParquetFile(path);
	}
	/**
	 * 计算逻辑回归函数,将数据帕映射到(0,1)
	 * 此时x值在0-12范围外无明显变化
	 * @param x
	 * @return  
	 */
	public double Sigmoid(double x){
		//将函数左移
		double ex=Math.pow(Math.E,(x-6));
		return ex/(1+ex);
	}


	public String getCaseProductId(String kvStr,String column){
		String[] strs=kvStr.split(",");
		Map<String,String> map=new HashMap<String,String>();

		if(strs!=null&&strs.length>0){
			for (String s : strs) {

				String[] ss=s.split(":");
				if(ss.length>=2){
					String s1=s.split(":")[0];
					String s2=s.split(":")[1];
					map.put(s1,s2);
				}
			}
		}
		String resultStr="case ";
		for( Map.Entry<String,String> a:map.entrySet()) {
			String key=a.getKey();
			String value=a.getValue();
			resultStr+=" when "+column+"='"+key+"' then '"+value+"' ";
//			resultStr+=" when subtype='"+key+"' then '"+value+"' ";
		}
		resultStr+=" else '' end as lrt_id";
		return resultStr;
	}
	/**
	 *and column in('key/value','key/value')
	 * @param kvStr key:value 形式的字符串
	 * @param column 数据库字段
	 * @param useKey 是 使用key 还是value
	 * @return
	 */
	private String getProductIn(String kvStr,String column,boolean useKey){
		String[] strs=kvStr.split(",");
		Map<String,String> map=new HashMap<String,String>();

		if(strs!=null&&strs.length>0){
			for (String s : strs) {

				String[] ss=s.split(":");
				if(ss.length>=2){
					String s1=s.split(":")[0];
					String s2=s.split(":")[1];
					map.put(s1,s2);
				}
			}
		}
		String resultStr="and "+column+" in ( ";
		for( Map.Entry<String,String> a:map.entrySet()) {
			String key=a.getKey();
			String value=a.getValue();
			if(useKey){
				resultStr+=" '"+key+"',";
			}else {
				resultStr+=" '"+value+"',";
			}
		}
		resultStr=resultStr.substring(0,resultStr.length()-1);
		resultStr+=" )";
		return resultStr;
	}

	public static void main(String[] args) {
		RecomengWithoutAlgo rwz=new RecomengWithoutAlgo();
		String visit_duration ="600.804";
		String visit_count ="2";
		/*
		 * duration_D 0-600  
		 * count_D   0-60
		 * 将它们缩放到  0-12
		 */
		double duration_D=Double.valueOf(visit_duration);
		double count_D=Double.valueOf(visit_count);
		/*
		 * duration_D 0-600  
		 * count_D   0-12
		 * 将它们缩放到  0-12
		 */
		double x1=duration_D/50;
		double x2=count_D;
		/*
		 * 计算 得分。百分制。  比重 7：3
		 * 分 0-100
		 */
		double percent_d=Double.valueOf(70)/100;
		
		double score=100*(rwz.Sigmoid(x1)*(1-percent_d)+rwz.Sigmoid(x2)*percent_d);
		System.out.println(score);
		
		DataFrame df=null;
		JavaRDD<Row> user_id_jRDD=df.rdd().toJavaRDD();
		
	}
	

}
