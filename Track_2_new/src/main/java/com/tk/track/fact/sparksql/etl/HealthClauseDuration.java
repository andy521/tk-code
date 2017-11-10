package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

/**
 * @author itw_shayl 健康告知页面停留时常统计 2017-7-18
 */
public class HealthClauseDuration implements Serializable {
	private static final long serialVersionUID = 1L;
	
	@SuppressWarnings("unused")
	public DataFrame getHealthDuration(HiveContext hiveContext) {
		//01:数据准备:大健康表
		DataFrame FH = getFactHealthcreditscoreresultNotNullDF(hiveContext,"fact_healthcreditscoreresult","FactHealthcreditscoreResultUserIdNotNullDF");
		//02:数据准备:计算出财险用户保单信息
		DataFrame PI = getPropertyInsurancePolicy(hiveContext,"PropertyInsurancePolicy");
		//03:数据准备:关联财险用户表和寿险投保人表，得到用户信息
		DataFrame SI = getSummaryInsurancePolicy(hiveContext,"InsurancePolicy");
		//04:统计出所有进入过"支付页面"的"终端(terminal_id)"
		DataFrame TP = getTerminalPayed(hiveContext,"TerminalPayed");
		//05:根据terminal_id找到所有的浏览记录
		DataFrame ED = getEventDataFilter(hiveContext,"TerminalPayed","EventDataFilter");
		//06:统计个人信息最大最小时间
		DataFrame SPE = getStaticticPersonal(hiveContext,"EventDataFilter","PersonalDuration");
		//07:统计健康告知最大最小时间
		DataFrame SH = getStatisticHealth(hiveContext,"EventDataFilter","HealthDuration");
		//08:统计支付页面最大最小时间
		DataFrame SPA = getStatisticPay(hiveContext,"EventDataFilter","PayDuration");
		//09:汇总个人信息最大最小时间、健康告知最大最小时间、支付页面最大最小时间
		DataFrame SD = getSummarDuration(hiveContext,"PersonalDuration","HealthDuration","PayDuration","SummarDuration");
		//10:计算页面停留时间
		DataFrame FD = getFinalDuration(hiveContext,"SummarDuration","FinalDuration");
		//11:关联大健康表
		DataFrame FHD = connentFactHealthDataFrame(hiveContext,"FinalDuration","FactHealthcreditscoreResultUserIdNotNullDF","ConnentedFactHealthDataFrame");
		//12:关联财险,寿险表
		DataFrame CPD = connectPolicyInfoDataFrame(hiveContext,"ConnentedFactHealthDataFrame","InsurancePolicy","CompleteHealthDuration");
		
//		AppUtil.saveAsParquet(SI, "/user/tkonline/taikangtrack_test/data/health_clause/SI");
//		AppUtil.saveAsParquet(SI, "/user/tkonline/taikangtrack_test/data/health_clause/TP");
//		AppUtil.saveAsParquet(SI, "/user/tkonline/taikangtrack_test/data/health_clause/ED");
//		AppUtil.saveAsParquet(SPE, "/user/tkonline/taikangtrack_test/data/health_clause/SPE");
//		AppUtil.saveAsParquet(SH, "/user/tkonline/taikangtrack_test/data/health_clause/SH");
//		AppUtil.saveAsParquet(SPA, "/user/tkonline/taikangtrack_test/data/health_clause/SPA");
//		AppUtil.saveAsParquet(SD, "/user/tkonline/taikangtrack_test/data/health_clause/SD");
//		AppUtil.saveAsParquet(FD, "/user/tkonline/taikangtrack_test/data/health_clause/FD");
//		AppUtil.saveAsParquet(FHD, "/user/tkonline/taikangtrack_test/data/health_clause/FHD");
//		AppUtil.saveAsParquet(CPD, "/user/tkonline/taikangtrack_test/data/health_clause/CPD");
		return CPD;
	}
	
	/**
	 * 01
	 * 将大健康表空id转化,防止倾斜
	 */
    public DataFrame getFactHealthcreditscoreresultNotNullDF(HiveContext hiveContext,String fromTableName,String tableName) {
        String sql =
    		"select case                                               "+
			"         when member_id = '' or member_id is null then    "+
			"          genrandom('MEMBER_ID_')                         "+
			"         else                                             "+
			"          member_id                                       "+
			"       end as member_id,                                  "+//member_id
			"       case                                               "+
			"         when open_id = '' or open_id is null then        "+
			"          genrandom('OPEN_ID_')                           "+
			"         else                                             "+
			"          open_id                                         "+
			"       end as open_id,                                    "+//open_id
			"       case                                               "+
			"         when customer_id = '' or customer_id is null then"+
			"          genrandom('CUSTOMER_ID_')                       "+
			"         else                                             "+
			"          customer_id                                     "+
			"       end as customer_id,                                "+//customer_id
			"       case                                               "+
			"         when cid_number = '' or cid_number is null then  "+
			"          genrandom('CID_NUMBER_')                        "+
			"         else                                             "+
			"          cid_number                                      "+
			"       end as cid_number,                                 "+//cid_number
			"       case                                               "+
			"         when name = '' or name is null then              "+
			"          genrandom('NAME')                               "+
			"         else                                             "+
			"          name                                            "+
			"       end as name,                                       "+//name
			"       user_id                                            "+
			"  from "+fromTableName+"                                  "+
			" where (customer_id is not null and customer_id <> '')    "+
			"    or (cid_number is not null and cid_number <> '')      ";
        return DataFrameUtil.getDataFrame(hiveContext, sql, tableName,DataFrameUtil.CACHETABLE_PARQUET);
    }

	/**
	 * 02
	 * @param hiveContext
	 * @param tableName
	 * @return
	 */
    //财险数据准备  - 01
    private DataFrame getPropertyInsurancePolicy(HiveContext hiveContext,String tableName){
    	String sql = 
			"select p.policyno,               "+
			"       p.sumgrosspremium,        "+
			"       p.operatedate,            "+
			"       c.splancode,              "+
			"       p.surveyind as from_id    "+
			"  from tkubdb.gschannelplancode c"+//HBase 数据小
			" inner join GUPOLICYCOPYMAIN p   "+
			"    on c.dplancode = p.flowid    "+
			" where c.dplancode is not null   "+
			"   and p.flowid is not null      ";
    	return DataFrameUtil.getDataFrame(hiveContext, sql, tableName);
    }
    /**
     * 03
     * @param hiveContext
     * @param tableName
     * @return
     */
    //财险数据准备 - 02
    private DataFrame getSummaryInsurancePolicy(HiveContext hiveContext,String tableName) {
        String sql = 
    		"select distinct p.policyno        as lia_policyno,  "+
			"                p.sumgrosspremium as premium,       "+
			"                p.operatedate     as lia_accepttime,"+
			"                p.splancode       as lrt_id,        "+
			"                p.from_id,                          "+
			"                g.identifynumber,                   "+//去大健康里找
			"                g.insuredname     as name,          "+
			"                g.insuredflag                       "+
			"  from GUPOLICYRELATEDPARTY g                       "+
			" inner join PropertyInsurancePolicy p               "+
			"    on p.policyno = g.policyno                      "+
			" where g.identifynumber is not null                 "+
			"   and g.insuredname is not null                    "+
			"   and g.insuredflag = '1'                          ";
        return DataFrameUtil.getDataFrame(hiveContext, sql, tableName);
    }
    
    /**
     * 04
     * @param hiveContext
     * @param tableName
     * @return
     */
	private DataFrame getTerminalPayed(HiveContext hiveContext,String tableName) {
		String sql = 
			"select distinct terminal_id "+
			"  from UBA_LOG_EVENT        "+
			" where app_type = 'javaWeb' "+
			"   and app_id = 'javaWeb001'"+
			"   and event = '支付页面'      ";
		return DataFrameUtil.getDataFrame(hiveContext, sql, tableName);
	}

	/**
	 * 05
	 * lrtId:([0-9]+):产品编号为数字
	 */
	private DataFrame getEventDataFilter(HiveContext hiveContext,String joinTableName,String tableName) {
		String sql =
			"select a.terminal_id,                                                       "+
			"       a.user_id,                                                           "+
			"       a.clienttime as time,                                                "+//time
			"       from_unixtime(cast(cast(a.clienttime as bigint) / 1000 as bigint),   "+
			"                     'yyyy-MM-dd HH:mm:ss') as clienttime,                  "+//clienttime
			"       a.event,                                                             "+
			"       a.subtype,                                                           "+
			"       case                                                                 "+ 
			"         when regexp_extract(a.label, 'lrtId:([0-9]+)') is not null and     "+
			"              regexp_extract(a.label, 'lrtId:([0-9]+)') <> '' then          "+
			"          regexp_extract(a.label, 'lrtId:([0-9]+)')                         "+
			"         when regexp_extract(a.label, 'lrtId:\"([0-9]+)\"') is not null and "+
			"              regexp_extract(a.label, 'lrtId:\"([0-9]+)\"') <> '' then      "+
			"          regexp_extract(a.label, 'lrtId:\"([0-9]+)\"')                     "+
			"         when regexp_extract(a.label, 'lrtId:(S[0-9]+)') is not null and    "+
			"              regexp_extract(a.label, 'lrtId:(S[0-9]+)') <> '' then         "+
			"          regexp_extract(a.label, 'lrtId:(S[0-9]+)')                        "+
			"         when regexp_extract(a.label, 'lrtId:\"(S[0-9]+)\"') is not null and"+
			"              regexp_extract(a.label, 'lrtId:\"(S[0-9]+)\"') <> '' then     "+
			"          regexp_extract(a.label, 'lrtId:\"(S[0-9]+)\"')                    "+
			"       end as lrt_id,                                                       "+//lrt_id
			"       regexp_extract(a.label, '(?<=lrtName:)[^,]*', 0) as lrtName,         "+//lrtName
			"       case                                                                 "+
			"         when regexp_extract(a.label, 'formId:([0-9]+)') is not null and    "+
			"              regexp_extract(a.label, 'formId:([0-9]+)') <> '' then         "+
			"          regexp_extract(a.label, 'formId:([0-9]+)')                        "+
			"         else                                                               "+
			"          regexp_extract(a.label, 'formId:\"([0-9]+)\"')                    "+
			"       end as formId,                                                       "+//formId
			"       case                                                                 "+
			"         when regexp_extract(a.label, 'from_id:([0-9]+)') is not null and   "+
			"              regexp_extract(a.label, 'from_id:([0-9]+)') <> '' then        "+
			"          regexp_extract(a.label, 'from_id:([0-9]+)')                       "+
			"         else                                                               "+
			"          a.from_id                                                         "+
			"       end as from_id                                                       "+//from_id
			"  from UBA_LOG_EVENT a                                                      "+
			" inner join "+joinTableName+" b                                             "+
			"    on a.terminal_id = b.terminal_id                                        "+
			" where a.app_type = 'javaWeb'                                               "+
			"   and a.app_id = 'javaWeb001'                                              "+
			"   and a.user_id is not null                                                "+
			"   and a.user_id <> ''                                                      ";
		return DataFrameUtil.getDataFrame(hiveContext, sql,tableName);
	}

	/**
	 * 06
	 * @param hiveContext
	 * @param fromTable
	 * @param tableName
	 * @return
	 */
	private DataFrame getStaticticPersonal(HiveContext hiveContext,String fromTable,String tableName) {
		String sql = 
			"select                                                                 "+
			"       user_id,                                                        "+
			"       event,                                                          "+
			"       lrt_id,                                                         "+
			"       lrtName,                                                        "+
			"       formId,                                                         "+
			"       from_id,                                                        "+
			"       min(clienttime) as personal_duration_min,                       "+
			"       max(clienttime) as personal_duration_max                        "+
			"  from "+fromTable+"                                                   "+
			" where event = '个人信息'                                                 "+
			" group by user_id,event, lrt_id, lrtName, formId, from_id";
		return DataFrameUtil.getDataFrame(hiveContext, sql, tableName,DataFrameUtil.CACHETABLE_PARQUET);
	}

	/**
	 * 07
	 * @param hiveContext
	 * @param fromTable
	 * @param tableName
	 * @return
	 */
	private DataFrame getStatisticHealth(HiveContext hiveContext,String fromTable,String tableName) {
		String sql =
			"select                                                                 "+
			"       user_id,                                                        "+
			"       event,                                                          "+
			"       lrt_id,                                                         "+
			"       lrtName,                                                        "+
			"       formId,                                                         "+
			"       from_id,                                                        "+
			"       min(clienttime) as health_duration_min,                         "+
			"       max(clienttime) as health_duration_max                          "+
			"  from "+fromTable+"                                                   "+
			" where event = '健康告知'                                                 "+
			" group by user_id, event, lrt_id, lrtName, formId, from_id";
		return DataFrameUtil.getDataFrame(hiveContext, sql, tableName,DataFrameUtil.CACHETABLE_PARQUET);
	}

	/**
	 * 08
	 * @param hiveContext
	 * @param fromTable
	 * @param tableName
	 * @return
	 */
	private DataFrame getStatisticPay(HiveContext hiveContext,String fromTable,String tableName) {
		String sql =
			"select                                                                 "+
			"       user_id,                                                        "+
			"       event,                                                          "+
			"       lrt_id,                                                         "+
			"       lrtName,                                                        "+
			"       formId,                                                         "+
			"       from_id,                                                        "+
			"       min(clienttime) as pay_duration_min,                            "+
			"       max(clienttime) as pay_duration_max                             "+
			"  from "+fromTable+"                                                   "+
			" where event = '支付页面'                                                 "+
			" group by user_id, event, lrt_id, lrtName, formId, from_id";
		return DataFrameUtil.getDataFrame(hiveContext, sql, tableName,DataFrameUtil.CACHETABLE_PARQUET);
	}

	/**
	 * 09
	 * @param hiveContext
	 * @param personalTableName
	 * @param healthTableName
	 * @param payTableName
	 * @param tableName
	 * @return
	 */
	private DataFrame getSummarDuration(HiveContext hiveContext,String personalTableName,String healthTableName,String payTableName,String tableName) {
		String sql =
			"select                              "+
			"       a.user_id,                   "+
			"       a.lrt_id,                    "+
			"       a.lrtName,                   "+
			"       a.formId,                    "+
			"       a.from_id,                   "+
			"       a.personal_duration_min,     "+
			"       a.personal_duration_max,     "+
			"       b.health_duration_min,       "+
			"       b.health_duration_max,       "+
			"       c.pay_duration_min,          "+
			"       c.pay_duration_max           "+
			"  from PersonalDuration a,  "+
			"       HealthDuration   b,  "+
			"       PayDuration      c   "+
			" where                               "+
			"        a.user_id = b.user_id        "+
			"   and a.formId = b.formId          "+
			"   and a.from_id = b.from_id        "+
			"   and c.user_id = b.user_id        "+
			"   and c.formId = b.formId          "+
			"   and c.from_id = b.from_id        ";
		return DataFrameUtil.getDataFrame(hiveContext, sql, tableName);
	}
	
	/**
	 * 10
	 * @param hiveContext
	 * @param fromTable
	 * @param tableName
	 * @return
	 */
	private DataFrame getFinalDuration(HiveContext hiveContext,String fromTable,String tableName){
		String sql = 
			"select user_id,                                              "+
			"       lrt_id,                                               "+
			"       lrtName,                                              "+
			"       formid,                                               "+
			"       from_id,                                              "+
			"       from_unixtime(-28800+(unix_timestamp(personal_duration_max)-unix_timestamp(personal_duration_min)),'HH:mm:ss') as personal_duration,"+
			"       from_unixtime(-28800+(unix_timestamp(health_duration_max)-unix_timestamp(personal_duration_min)),'HH:mm:ss') as health_duration,"+
			"       from_unixtime(-28800+(unix_timestamp(pay_duration_max)-unix_timestamp(pay_duration_min)),'HH:mm:ss') as pay_duration"+
			"  from "+fromTable+"                                         ";
		return DataFrameUtil.getDataFrame(hiveContext, sql, tableName);
	}
	
	/**
	 * 11
	 * @param hiveContext
	 * @param finalDuration
	 * @param factHealthcreditscoreResultUserIdNotNullDF
	 * @param tableName
	 * @return
	 */
	private DataFrame connentFactHealthDataFrame(HiveContext hiveContext,String finalDuration,String factHealthcreditscoreResultUserIdNotNullDF,String tableName){
		String sql = 
			"select fh.user_id,                                       "+
			"       fd.lrt_id,                                        "+
			"       fd.lrtName,                                       "+
			"       fd.formid,                                        "+
			"       fd.from_id,                                       "+
			"       fd.personal_duration,                             "+
			"       fd.health_duration,                               "+
			"       fd.pay_duration,                                  "+
			"       fh.open_id,                                       "+
			"       fd.user_id member_id,                             "+
			"       fh.customer_id,                                   "+
			"       fh.cid_number,                                    "+
			"       fh.name                                           "+
			"  from "+finalDuration+" fd                              "+
			"  inner join "+factHealthcreditscoreResultUserIdNotNullDF+" fh"+
			"    on fd.user_id = fh.member_id                         ";
		return DataFrameUtil.getDataFrame(hiveContext, sql, tableName,DataFrameUtil.CACHETABLE_PARQUET);
	}
	
    
    /**
     * 12
     * 关联链路：关联大健康表、关联寿险（on customer_id），关联财险(on cid_number)
     */
    private DataFrame connectPolicyInfoDataFrame(HiveContext hiveContext,String connentedFactHealthDataFrame,String InsurancePolicy,String tableName){
    	String sql = 
    		//关联寿险开始
		    "select distinct fd.user_id,                                               "+
		    "       fd.lrt_id,                                                         "+
		    "       fd.lrtName,                                                        "+
		    "       fd.formid,                                                         "+
		    "       fd.from_id,                                                        "+
			"       fd.personal_duration,                                              "+
			"       fd.health_duration,                                                "+
			"       fd.pay_duration,                                                   "+
			"       fd.open_id,                                                        "+
			"       fd.member_id,                                                      "+
			"       fd.customer_id,                                                    "+
			"       fd.cid_number,                                                     "+
    		"       fp.lia_policyno,                                                   "+
    		"       fp.lia_accepttime,                                                 "+
    		"       fp.lia_premium premium                                             "+
    		"  from "+connentedFactHealthDataFrame+" fd                                "+
    		" inner join f_net_policysummary fp                                        "+
			"    on fd.customer_id = fp.policyholder_id                                "+
    		//关联寿险结束
    		" union all "+
    		//关联财险开始
		    "select distinct fd.user_id,                                               "+
		    "       fd.lrt_id,                                                         "+
		    "       fd.lrtName,                                                        "+
		    "       fd.formid,                                                         "+
		    "       fd.from_id,                                                        "+
			"       fd.personal_duration,                                              "+
			"       fd.health_duration,                                                "+
			"       fd.pay_duration,                                                   "+
			"       fd.open_id,                                                        "+
			"       fd.member_id,                                                      "+
			"       fd.customer_id,                                                    "+
			"       fd.cid_number,                                                     "+
			"       fh.lia_policyno,                                                   "+
			"       fh.lia_accepttime,                                                 "+
    	    "       fh.premium                                                         "+
    	    "  from "+connentedFactHealthDataFrame+" fd                                "+
    	    " inner join "+InsurancePolicy+" fh                                        "+
    	    "    on fh.identifynumber = fd.cid_number                                  "+
    	    "   and fh.name = fd.name                                                  "+
    	    "   and fh.lrt_id = fd.lrt_id                                              ";
//    	    " where unix_timestamp(fh.lia_accepttime) >= cast(fd.time / 1000 as bigint)"; --TODO 使用最大时间（max_time）
    		//关联财险结束
    	return DataFrameUtil.getDataFrame(hiveContext, sql, tableName);
    }
    
	public void saveAsParquet(DataFrame DF,String path){
		TK_DataFormatConvertUtil.deletePath(path);
		DF.saveAsParquetFile(path);
	}
}
