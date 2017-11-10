package com.tk.track.fact.sparksql.etl;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by t-chenhao01 on 2016/11/25.
 */
public class RecommendWithoutAlgoEvaluate_new implements Serializable {
    private static final long serialVersionUID = -2892290110975634848L;

    private static final String productKvMap = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_RECOMEND_EVALUATE_PRODUCTMAP);


    /**
     * 得到小康推荐页面的访问，并根据插码描述补上险种编号
     * 量级 241128
     * @param sqlContext
     * @return
     */
    private DataFrame getRecommendWxPageVisit(HiveContext sqlContext){
        String caseWhenPid=getCaseProductId(productKvMap,"subtype");
        String caseWhenPName=getCaseProductName(productKvMap, "subtype");
        String caseWhenPIn=getProductIn(productKvMap, "subtype",true);
        String sql= "select terminal_id,user_id,subtype,time,event, " +
//                "case when subtype='体检保' then 'S20160091'  " +
//                " when subtype='少儿重疾' then 'S20160159'  " +
//                " when subtype='成人重疾' then 'S20160158'   " +
//                " when subtype='微互助求关爱' then '241'   " +
//                " when subtype='综合意外' then '239'   " +
//                " when subtype='老年癌症医疗保险' then 'S20160266'   " +
//                " when subtype='定期寿险' then '281'  " +
//                " when subtype='e享健康返本型重疾险' then '298'   " +
//                " when subtype='住院保' then 'S20160203'  " +
//                " when subtype='E理财B投连险' then '522'  " +
//                " when subtype='门急诊住院报销' then '282'  " +
//                " when subtype='万里无忧B款' then '297'  " +
//                " when subtype='短期旅行意外' then '1103'  " +
//                //乳腺癌患者绿通服务 产品特殊，不列入推荐
//                " when subtype='乳腺癌患者绿通服务' then ''  " +
//                " when subtype='健康1+1返本型重疾险' then '290'  " +
//                " when subtype='悦享中华高端医疗费用报销' then '249' else ''  " +
//                " end as lrt_id,  " +
                caseWhenPid+","+
                caseWhenPName+","+
//                "case when subtype='体检保' then '体检保'  " +
//                " when subtype='少儿重疾' then '少儿重疾'  " +
//                " when subtype='成人重疾' then '成人重疾'   " +
//                " when subtype='微互助求关爱' then '微互助求关爱'   " +
//                " when subtype='综合意外' then '综合意外'   " +
//                " when subtype='老年癌症医疗保险' then '老年癌症医疗保险'   " +
//                " when subtype='定期寿险' then '定期寿险'  " +
//                " when subtype='e享健康返本型重疾险' then 'e享健康返本型重疾险'   " +
//                " when subtype='住院保' then '住院保'  " +
//                " when subtype='E理财B投连险' then 'E理财B投连险'  " +
//                " when subtype='门急诊住院报销' then '门急诊住院报销'  " +
//                " when subtype='万里无忧B款' then '万里无忧B款'  " +
//                " when subtype='短期旅行意外' then '短期旅行意外'  " +
//                //乳腺癌患者绿通服务 产品特殊，不列入推荐
//                " when subtype='乳腺癌患者绿通服务' then ''  " +
//                " when subtype='健康1+1返本型重疾险' then '健康1+1返本型重疾险'  " +
//                " when subtype='悦享中华高端医疗费用报销' then '悦享中华高端医疗费用报销' else ''  " +
//                " end as product_name  " +
                  " '0' as info_type  " +
                " from uba_log_event where event='小康推荐' and user_id is not null and user_id <>'' " +
                "and subtype is not null " +
                caseWhenPIn;
//                "and subtype in " +
//                "('体检保','少儿重疾','微互助求关爱','成人重疾','综合意外','老年癌症医疗保险','定期寿险'," +
//                "'e享健康返本型重疾险','住院保','E理财B投连险','门急诊住院报销','万里无忧B款','短期旅行意外'," +
//                "'健康1+1返本型重疾险','悦享中华高端医疗费用报销')  ";

        return DataFrameUtil.getDataFrame(sqlContext, sql, "RecommendWxPageVisit");

    }


    /**
     * 得到wap首页推荐页面的访问，
     *
     * @param sqlContext
     * @return
     */
    private DataFrame getRecommenWapPageVisit(HiveContext sqlContext){
    	 String sql = "select tul.terminal_id, "
                 + "           ftm.user_id, tul.subtype, tul.time, tul.event, "
                 + "      case when regexp_extract(label,'lrtId:\"([0-9]+)\"') is not null and  regexp_extract(label,'lrtId:\"([0-9]+)\"') <> '' then regexp_extract(label,'lrtId:\"([0-9]+)\"')"
                 + "           else regexp_extract(label,'lrtId:\"(S[0-9]+)\"')"
                 +"        end as  lrt_id ,   "
                 + "           tul.subtype as product_name, "
                 + "          '1' as info_type "
                 + "         from uba_log_event  tul "
                 + "      inner join f_terminal_map ftm "
                 + "            on tul.terminal_id=ftm.terminal_id and tul.user_id=ftm.user_id "
                 + "	        where tul.event = '首页推荐' "
                 + "	         and tul.app_Type = 'H5' "
                 + "	        and tul.app_Id = 'clue_H5_website_001' ";
    	 return DataFrameUtil.getDataFrame(sqlContext, sql, "RecommendWapPageVisit");
    }




    /**
     * 得到pc首页推荐页面的访问，
     * @param sqlContext
     * @return
     */
    private DataFrame getRecommendPcPageVisit(HiveContext sqlContext){

    	 String sql = "select tul.terminal_id, "
                 + "           ftm.user_id, tul.subtype, tul.time, tul.event, "
                 + "      case when regexp_extract(label,'lrtId:\"([0-9]+)\"') is not null and  regexp_extract(label,'lrtId:\"([0-9]+)\"') <> '' then regexp_extract(label,'lrtId:\"([0-9]+)\"')"
                 + "           else regexp_extract(label,'lrtId:\"(S[0-9]+)\"')"
                 +"        end as  lrt_id ,   "
                 + "           tul.subtype as product_name, "
                 + "          '2' as info_type "
                 + "         from uba_log_event  tul "
                 + "      inner join f_terminal_map ftm "
                 + "            on tul.terminal_id=ftm.terminal_id and tul.user_id=ftm.user_id "
                 + "	        where tul.event = '首页推荐' "
                 + "	         and tul.app_Type = 'webSite' "
                 + "	        and tul.app_Id = 'webSite003' ";
    	 return DataFrameUtil.getDataFrame(sqlContext, sql, "RecommendPcPageVisit");
    }

    /**
     * WAP和pc端数据合并
     * @param sqlContext
     * @return
     */
    private DataFrame getMergePcAndWapPageVisit(HiveContext sqlContext,String tableName1,String tableName2,String destTableName){
    	 String sql = " select terminal_id, "
    			 +"       user_id, "
    			 +"       subtype, "
    			 +"       time, "
    			 +"       event, "
    			 +"       lrt_id, "
    			 +"       product_name, "
    			 +"       info_type "
    			 +"  from " +tableName1
    			 +" union all "
    			 +" select terminal_id, "
    			 +"       user_id, "
    			 +"       subtype, "
    			 +"       time, "
    			 +"       event, "
    			 +"       lrt_id, "
    			 +"       product_name, "
    			 +"       info_type "
    			 +"  from "+tableName2 ;

    	 return DataFrameUtil.getDataFrame(sqlContext, sql, destTableName);
    }


    /**
     * 当源表中user_id 为open_id 时关联fact_healthcreditscoreresult 取通用的 userid，原userId作为open_id
     * @param sqlContext
     * @param tableName
     * @return
     */
    private DataFrame getOpenIdAndUserID(HiveContext sqlContext,String tableName,String destTableName,String isRecomend) {
        String sql = "SELECT /*+MAPJOIN(orgtable)*/ orgtable.terminal_id,h.user_id,h.member_id,orgtable.user_id open_id," +
                "orgtable.lrt_id,orgtable.subtype,orgtable.time,orgtable.event,orgtable.product_name,orgtable.info_type,'"+isRecomend+"' as is_recomend "
                + "	from "+tableName+" orgtable left join FactHealthcreditscoreResultUserIdNotNullDF h"
                + "	on orgtable.user_id=h.open_id";

        return DataFrameUtil.getDataFrame(sqlContext, sql, destTableName, DataFrameUtil.CACHETABLE_PARQUET);
    }
    /**
     *
     * @param sqlContext
     * @return
     */
    private DataFrame getCommenWapPageVisit(HiveContext sqlContext){

    	String sql= "  select user_id, lrt_id  "
  			  +"      from RecommendWapPageVisit  group by user_id,lrt_id" ;

    	return DataFrameUtil.getDataFrame(sqlContext, sql, "wap_CommonUserBrowseInfos");
    }


    /**
     *
     * @param sqlContext
     * @return
     */
    private DataFrame getWapUserBrowseInfos(HiveContext sqlContext){
    	String sql= "  select tul.terminal_id, "
  			  +"   ftm.user_id,tul.subtype,tul.time,tul.event,"
  			  + "      case when regexp_extract(tul.label,'lrtID:\"([0-9]+)\"') is not null and regexp_extract(tul.label,'lrtID:\"([0-9]+)\"') <> '' then regexp_extract(tul.label,'lrtID:\"([0-9]+)\"')"
                + "           else regexp_extract(tul.label,'lrtID:\"(S[0-9]+)\"')"
                +"        end as  lrt_id ,   "
  			  +"      regexp_extract(tul.label,'(?<=lrtName:\")[^\",]*',0) as product_name,"
  			  +"      '1' as info_type "
  			  +" from uba_log_event  tul "
  			  +" inner join f_terminal_map ftm "
  			  +"  on tul.terminal_id=ftm.terminal_id and tul.user_id=ftm.user_id "
  			  +"  where tul.event = '商品详情' and tul.app_Type = 'H5' "
  			  +"  and tul.app_Id = 'clue_H5_mall_001' "
  			  +" union all "
  			  +"  select tul.terminal_id, "
  			  +"   ftm.user_id,tul.subtype,tul.time,tul.event,"
  			  + "      case when regexp_extract(tul.label,'productId:\"([0-9]+)\"') is not null and regexp_extract(tul.label,'productId:\"([0-9]+)\"') <> '' then regexp_extract(tul.label,'productId:\"([0-9]+)\"')"
                + "           else regexp_extract(tul.label,'productId:\"(S[0-9]+)\"')"
                +"        end as  lrt_id ,   "
  			  +"     regexp_extract(tul.label,'(?<=lrtName:\")[^\",]*',0) as product_name,"
  			  +"      '1' as info_type  "
  			  +" from uba_log_event  tul "
  			  +" inner join f_terminal_map ftm "
  			  +"  on tul.terminal_id=ftm.terminal_id and tul.user_id=ftm.user_id "
  			  +"  where tul.event = '商品详情' and tul.app_Type = 'H5' "
  			  +"  and tul.app_Id = 'H5_insure_flow' ";
    	return DataFrameUtil.getDataFrame(sqlContext, sql, "wap_UserBrowseInfos");
    }


    private DataFrame getNonRecommendWapPageVisit(HiveContext sqlContext){
    	String sql="    select t.terminal_id, "
    			+"       t.user_id, "
    			+"       t.subtype, "
    			+"       t.time, "
    			+"       t.event,  "
    			+"       t.lrt_id, "
    			+"       t.product_name, "
    			+"       t.info_type "
    			+"  from (select wu.terminal_id, "
    			+"               wu.user_id, "
    			+"               wu.subtype, "
    			+"               wu.time, "
    			+"               wu.event, "
    			+"               wu.lrt_id, "
    			+"               wu.product_name, "
    			+"               wu.info_type, "
    			+"               rep.user_id as user_idrep,"
    			+"               rep.lrt_id as lrt_idrep "
    			+"          from wap_UserBrowseInfos wu "
    			+"          left join wap_CommonUserBrowseInfos rep "
    			+"            on wu.user_id = rep.user_id  and wu.lrt_id = rep.lrt_id ) t "
    			+"   where (t.user_idrep is null or t.user_idrep = '') "
    			+"   and (t.lrt_idrep is null or t.lrt_idrep = '') ";
    	return DataFrameUtil.getDataFrame(sqlContext, sql, "NonRecommendWapPageVisit");
    }

    /**
     *
     * @param sqlContext
     * @return
     */
    private DataFrame getPcNewUserBrowseInfos(HiveContext sqlContext){

    	String sql= "select t.terminal_id,t.user_id,t.subtype,t.time,t.event,t.lrt_id,t.product_name,t.info_type"
    			  +"  from  (select tul.terminal_id, "
    			  +"   ftm.user_id,tul.subtype,tul.time,tul.event,"
    			  + "      case when regexp_extract(tul.label,'code:\"([0-9]+)\"') is not null and regexp_extract(tul.label,'code:\"([0-9]+)\"') <> '' then regexp_extract(tul.label,'code:\"([0-9]+)\"')"
                  + "           else regexp_extract(tul.label,'code:\"(S[0-9]+)\"')"
                  +"        end as  lrt_id ,   "
    			  +"      regexp_extract(tul.label,'(?<=productName:\")[^\",]*',0) as product_name,"
    			  +"      '2' as info_type "
    			  +" from uba_log_event  tul "
    			  +" inner join f_terminal_map ftm "
    			  +"  on tul.terminal_id=ftm.terminal_id and tul.user_id=ftm.user_id "
    			  +"  where tul.event = '商品详情' and tul.app_Type = 'mall' "
    			  +"  and tul.app_Id = 'mall001') t "
    			  +" where t.lrt_id is not null and t.lrt_id <> '' ";

    	return DataFrameUtil.getDataFrame(sqlContext, sql, "pc_newUserBrowseInfos");
    }

    /**
     *
     * @param sqlContext
     * @return
     */
    private DataFrame getCommenPcPageVisit(HiveContext sqlContext){
    	String sql= "  select user_id, lrt_id "
    			  +"      from RecommendPcPageVisit   "
    			  +" group by  user_id ,lrt_id";
    	return DataFrameUtil.getDataFrame(sqlContext, sql, "pc_CommonUserBrowseInfos");
    }

    /**
     *
     * @param sqlContext
     * @return
     */
    private DataFrame getNonRecommendPcPageVisit(HiveContext sqlContext){
    	String sql="     select t.terminal_id,"
    			+"       t.user_id, "
    			+"       t.subtype, "
    			+"       t.time, "
    			+"       t.event, "
    			+"       t.lrt_id, "
    			+"       t.product_name, "
    			+"       t.info_type "
    			+"  from (select wu.terminal_id, "
    			+"               wu.user_id, "
    			+"               wu.subtype, "
    			+"               wu.time, "
    			+"               wu.event, "
    			+"               wu.lrt_id, "
    			+"               wu.product_name, "
    			+"               wu.info_type, "
    			+"               rep.user_id as user_idrep, "
    			+"			   rep.lrt_id as lrt_idrep "
    			+"          from pc_UserBrowseInfos wu "
    			+"          left join pc_CommonUserBrowseInfos rep "
    			+"            on wu.user_id = rep.user_id and wu.lrt_id = rep.lrt_id) t "
    			+" where (t.user_idrep is null or t.user_idrep = '') "
    			+"   and (t.lrt_idrep is null or t.lrt_idrep = '') ";
    	return DataFrameUtil.getDataFrame(sqlContext, sql, "NonRecommendPcPageVisit");
    }

    /**
     * 将大健康表空id转化,防止倾斜
     * @param sqlContext
     * @return
     */
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
     * 当源表中user_id 为open_id 时关联fact_healthcreditscoreresult 取通用的 userid，原userId作为open_id
     * @param sqlContext
     * @param tableName
     * @return
     */
    private DataFrame getPcIdAndWapIdAndUserID(HiveContext sqlContext,String tableName,String destTableName,String isRecomend) {
        String sql = "SELECT /*+MAPJOIN(orgtable)*/ orgtable.terminal_id,h.user_id,orgtable.user_id member_id,h.open_id," +
                "orgtable.lrt_id,orgtable.subtype,orgtable.time,orgtable.event,orgtable.product_name,orgtable.info_type,'"+isRecomend+"' as is_recomend "
                + "	from "+tableName+" orgtable left join FactHealthcreditscoreResultUserIdNotNullDF h"
                + "	on orgtable.user_id=h.member_id";

        return DataFrameUtil.getDataFrame(sqlContext, sql,destTableName);

    }


    /**
     * 合并三端的数据
     * @param sqlContext
     * @param tableName1
     * @return
     */
    private DataFrame getRecommendAllUserID(HiveContext sqlContext,String tableName1,String tableName2,String destTableName ) {
        String sql = " select terminal_id, "
        		+"       user_id, "
        		+"       member_id, "
        		+"       open_id, "
        		+"       subtype, "
        		+"       time, "
        		+"       event, "
        		+"       lrt_id, "
        		+"       product_name, "
        		+"       info_type, "
        		+"       is_recomend "
        		+"   from "+tableName1
        		+" union all "
        		+" select terminal_id, "
        		+"       user_id, "
        		+"       member_id, "
        		+"       open_id, "
        		+"       subtype, "
        		+"       time, "
        		+"       event, "
        		+"      lrt_id, "
        		+"       product_name, "
        		+"       info_type, "
        		+"       is_recomend "
        		+"  from "+ tableName2 ;

        return DataFrameUtil.getDataFrame(sqlContext, sql, destTableName, DataFrameUtil.CACHETABLE_PARQUET);
    }
    /**
     * FACT_USERINFO 用户表中有客户有两条及以上记录，需要合并，因后面以customer_id关联寿险保单，
     * 所以以customer_id聚合，对于有多个会员号的人，按会员号降序排序，取最新的一条记录做为此人的记录。
     * @param sqlContext
     * @return
     */
    private DataFrame getLifeInsureCustomerId(HiveContext sqlContext){
        String sql="select a.user_id_list[0] as user_id,member_id_list[0] as member_id,open_id_list[0] as open_id,customer_id from (" +
                "select collect_list(user_id)  user_id_list,collect_list(member_id) member_id_list, customer_id, " +
                "collect_list(open_id) open_id_list from (" +
                    "select user_id,member_id,customer_id,open_id from FACT_USERINFO where member_id is not null and member_id <>'' " +
                    " and customer_id is not null and customer_id<>''  order by member_id,open_id, user_id  " +
                    ") t group by customer_id " +
                ") a";
        return DataFrameUtil.getDataFrame(sqlContext, sql, "USER_INFO_UNIQUE");
    }
    /**
     *用户拥有的保单表
     * 大表，数量级亿   164682190
     * @param sqlContext
     * @return
     */
    private DataFrame getUserVaildPolicy(HiveContext sqlContext){
       // String caseWhenPIn=getProductIn(productKvMap, "lrt_id",false);
        String sql="select a.user_id,a.customer_id," +
                "a.member_id,a.open_id," +
                "a.LRT_ID,a.lia_policyno,a.lia_accepttime,a.lia_validperiodbegin,a.lia_validperiodwillend," +
                "cast(sum(a.premium) as string) as  premium,a.from_id " +
                " from (select  uinfo.user_id,uinfo.customer_id,  " +
                "uinfo.member_id,  " +
                "uinfo.open_id,  " +
                "fp.LRT_ID,  " +
                "fp.lia_policyno,  " +
                "fp.lia_accepttime,  " +
                "fp.lia_validperiodbegin,  " +
                "fp.lia_validperiodwillend,  " +
                "cast(fp.liac_premium as double)  as premium,fp.from_id " +
                "from USER_INFO_UNIQUE uinfo   " +
                "inner join (select LRT_ID,  " +
                "policyholder_id,  " +
                "lia_policyno,  " +
                "lia_accepttime,liac_premium,lia_validperiodbegin,lia_validperiodwillend, handle_clientid as from_id  " +
                "from f_net_policysummary   " +
                "where policyholder_id is not null " +
                " and liac_premium <> 0  and liac_premium <> '0' " +
                "and LRT_ID is not null) fp on fp.policyholder_id =uinfo.customer_id ) a " +
                " group by a.user_id,a.customer_id," +
                "a.member_id,a.open_id," +
                "a.LRT_ID,a.lia_policyno,a.lia_accepttime,a.lia_validperiodbegin,a.lia_validperiodwillend,a.from_id" ;
        return DataFrameUtil.getDataFrame(sqlContext, sql, "UserVaildPolicy");
    }

    private DataFrame getPolicyinstranceType(HiveContext sqlContext){
        String sql="select uvp.user_id,uvp.customer_id,  " +
                "uvp.member_id,  " +
                "uvp.open_id,  " +
                "uvp.LRT_ID,  " +
                "uvp.lia_policyno,  " +
                "uvp.lia_accepttime," +
                "it.type as insuranceType,uvp.premium,uvp.from_id " +
                "from UserVaildPolicy uvp left join tknwdb.instrance_type it on it.lrtid=uvp.LRT_ID";
        return DataFrameUtil.getDataFrame(sqlContext, sql, "vaildPolicyInstanceType");
    }

    /**
     * 找出在 从推荐页面承保的，已经使用过的保单号。后面找非推荐时要排除这些保单号
     * @param sqlContext
     * @return
     */
    private DataFrame getUsedPolicyNo(HiveContext sqlContext,String srcTable){
        String sql="select  lia_policyno  from "+srcTable+" group by lia_policyno";
        return DataFrameUtil.getDataFrame(sqlContext, sql, "UsedPolicyNo",DataFrameUtil.CACHETABLE_PARQUET);
    }

    /**
     * 从用户拥有的保单记录里排除已经使用过的
     * @return
     */
    private DataFrame getNonUsedPolicyNo(HiveContext sqlContext){
        String sql=" select t.user_id,t.customer_id,  " +
                "t.member_id,  " +
                "t.open_id,  " +
                "t.LRT_ID,  " +
                "t.lia_policyno,  " +
                "t.lia_accepttime, " +
                "t.insuranceType, " +
                "t.premium,t.from_id " +
                " from (select up.user_id,up.customer_id,  " +
                "up.member_id,  " +
                "up.open_id,  " +
                "up.LRT_ID,  " +
                "up.lia_policyno,  " +
                "up.lia_accepttime, " +
                "up.insuranceType,up.premium,"+
                "up.from_id " +
                "from SumUserVaildPolicy up left join UsedPolicyNo usedp on up.lia_policyno=usedp.lia_policyno " +
                " where (usedp.lia_policyno is null or usedp.lia_policyno='')  ) t ";
        return DataFrameUtil.getDataFrame(sqlContext, sql, "NonUsedPolicyNo");
    }

    /**
     * 关联用户保单表和用户访问记录表，找出承保时间在浏览时间之后的记录。
     * 此时一个保单可能会对应多条浏览记录，取时间最早的一条
     * @param sqlContext
     * @return
     */
    private DataFrame getUserPolicyConnectBrowser(HiveContext sqlContext,String tableName1,String tableName,String destName,String infoType){
        String sql="select rv.user_id,rv.member_id,rv.open_id,rv.lrt_id,rv.product_name,rv.time,rv.info_type,rv.terminal_id,rv.subtype,rv.event," +
                "up.lia_policyno,up.lia_accepttime,up.insuranceType,up.premium,up.customer_id,up.from_id " +
                " from "+tableName1+" up  inner join "+tableName+" rv on rv.info_type='"+infoType+"' and up.user_id=rv.user_id and " +
                "  up.lrt_id=rv.lrt_id where unix_timestamp(up.lia_accepttime) >= " +
                "cast(rv.time/1000 as bigint)";
        return DataFrameUtil.getDataFrame(sqlContext, sql, destName);
    }



    /**
     * 一个保单可能会对应多条浏览记录，取时间最晚的一条,
     * 记作某浏览行为对应的保单，这里的数量记为转化成保单的数量
     * @param sqlContext
     * @return
     */
    private DataFrame getEarliestAsResult(HiveContext sqlContext,String tableName,String destName,String is_recomend){
        String sql="select user_id,member_id,open_id,customer_id,lrt_id,product_name,info_type,lia_policyno,lia_accepttime," +
                "max(cast(time as bigint)) as visit_time,'"+is_recomend+"' as is_recomend,insuranceType,premium,from_id " +
                " from "+tableName+" group by " +
                "user_id,member_id,open_id,customer_id,lrt_id,product_name,lia_policyno,lia_accepttime,insuranceType,premium,info_type,from_id";
        return DataFrameUtil.getDataFrame(sqlContext, sql, destName,DataFrameUtil.CACHETABLE_PARQUET);
    }

    /**
     * 一个保单可能会对应多条浏览记录，取时间最晚的一条,
     * 记作某浏览行为对应的保单，这里的数量记为转化成保单的数量
     * @param sqlContext
     * @return
     */
    private DataFrame getPolicyAndTimeAsResult(HiveContext sqlContext,String tableName,String destName){
        String sql="select lia_policyno,max(visit_time) as visit_time from "+tableName+" group by lia_policyno";
        return DataFrameUtil.getDataFrame(sqlContext, sql, destName,DataFrameUtil.CACHETABLE_PARQUET);
    }
    private DataFrame getDistictPolicyAsResult(HiveContext sqlContext,String dirctionTable,String tableName ,String destName){
        String sql="select b.user_id,b.member_id,b.open_id,b.customer_id,b.lrt_id,b.product_name,b.info_type,a.lia_policyno,b.lia_accepttime,a.visit_time,b.is_recomend,b.insuranceType,b.premium,b.from_id" +
                " from "+dirctionTable+" a left join "+tableName+" b on a.lia_policyno=b.lia_policyno and a.visit_time=b.visit_time";
        return DataFrameUtil.getDataFrame(sqlContext, sql, destName,DataFrameUtil.CACHETABLE_PARQUET);
    }


    /**
     * 用户浏览对应保单，按lrt_id，day聚合 并算出成交保单量
     * @param sqlContext
     * @return
     */
    private DataFrame getPolicyUserIdGroup (HiveContext sqlContext,String tableName,String destName){
        String sql="select t1.lrt_id,t1.product_name,t1.is_recomend," +
                "t1.deal_day,t1.info_type, " +
                "count(1) as policy_num, " +
                "sum(premium) as total_premium, " +
                "sum(short_insurance) as short_insurance_num, " +
                "sum(long_insurance) as long_insurance_num, " +
                "sum(undirect_flag) as undirect_num, " +
                "sum(direct_flag) as direct_num, " +
                "sum(direct_short_num) as direct_short_num, " +
                "sum(undirect_short_num) as undirect_short_num, " +
                "sum(direct_long_num) as direct_long_num " +
                " from (" +
                "select lrt_id,product_name,is_recomend, info_type, " +
                "cast(from_unixtime(cast(unix_timestamp(lia_accepttime) as bigint), 'yyyy-MM-dd') as  string) deal_day,  " +
                "cast(premium as bigint) as premium,  "+
                "cast(short_insurance as bigint) as short_insurance,  "+
                "cast(long_insurance as bigint) as long_insurance,  "+
                "cast(undirect_flag as bigint) as undirect_flag,  "+
                "cast(direct_flag as bigint) as direct_flag,  "+
                "case when short_insurance='1' and direct_flag='1' then 1 else 0 end as direct_short_num, "+
                "case when short_insurance='1' and undirect_flag='1' then 1 else 0 end as undirect_short_num,  "+
                "case when long_insurance='1' and direct_flag='1' then 1 else 0 end as direct_long_num  "+

                "from "+tableName+" where " +
                "lrt_id is not null and lrt_id <>'' and " +
                "product_name is not null and product_name<>'' and " +
                "lia_accepttime is not null and lia_accepttime <>'' " +
                ") t1 " +
                "group by t1.lrt_id,t1.product_name,t1.is_recomend,t1.deal_day,t1.info_type ";
        return DataFrameUtil.getDataFrame(sqlContext, sql,destName);
    }

    /**
     * 用户每次浏览行为按lrt_id，day聚合 并算出每日pv
     * @param sqlContext
     * @return
     */
    private DataFrame getBrowserUserIdGroup(HiveContext sqlContext,String tableName,String destName){
        String sql="select " +
                "t2.lrt_id,t2.product_name,is_recomend," +
                "t2.click_day,t2.info_type," +
                "count(1) as pv   " +
                " from (" +
                "select lrt_id,product_name,is_recomend, info_type," +
                "cast(from_unixtime(cast((time) / 1000 as bigint), 'yyyy-MM-dd') as  string) click_day " +
                "  from "+tableName+" where " +
                "lrt_id is not null and lrt_id <>'' and " +
                "product_name is not null and product_name<>'' and " +
                "time is not null and time <>'' " +
                ") t2 " +
                " group by t2.lrt_id,t2.product_name,t2.click_day,t2.is_recomend,t2.info_type";
        return DataFrameUtil.getDataFrame(sqlContext, sql, destName);
    }

    /**
     *关联用户浏览行为和浏览行为对应保单，
     * @param sqlContext
     * @return
     */
    private DataFrame mergeAndGroupByProduct(HiveContext sqlContext,String tableName1,String tableName2,String destName){

        String sql="select b.lrt_id,b.is_recomend,b.product_name,b.click_day,b.info_type," +
                "case when b.pv is null  then 0 else b.pv end as pv," +
                "case when p.policy_num is null  then 0 else p.policy_num end as policy_num," +
                "case when p.total_premium is null  then 0 else p.total_premium end as total_premium," +
                "case when p.short_insurance_num is null  then 0 else p.short_insurance_num end as short_insurance_num," +
                "case when p.long_insurance_num is null  then 0 else p.long_insurance_num end as long_insurance_num," +
                "case when p.undirect_num is null  then 0 else p.undirect_num end as undirect_num," +
                "case when p.direct_num is null  then 0 else p.direct_num end as direct_num, " +
                "case when p.direct_short_num is null  then 0 else p.direct_short_num end as direct_short_num, " +
                "case when p.undirect_short_num is null  then 0 else p.undirect_short_num end as undirect_short_num, " +
                "case when p.direct_long_num is null  then 0 else p.direct_long_num end as direct_long_num " +
                " from " +
                " "+tableName1+" b  left join "+tableName2+" p on " +
                "p.lrt_id=b.lrt_id and p.product_name=b.product_name and p.deal_day=b.click_day " +
                "and p.is_recomend=b.is_recomend and p.info_type=b.info_type";
        return DataFrameUtil.getDataFrame(sqlContext, sql, destName);
    }

    /**
     * 按产品 聚合得到最终结果
     * @param sqlContext
     * @return
     */
    private DataFrame getFinalGroupResult(HiveContext sqlContext,String tableName,String destName){

        String sql="select t.lrt_id,t.product_name,t.is_recomend,t.click_day,t.info_type,t.pv," +
                "t.deal_num, " +
                "t.total_premium,"+
                "t.short_insurance_num,"+
                "t.long_insurance_num,"+
                "t.direct_num,"+
                "t.undirect_num, "+
                "t.direct_short_num, "+
                "t.undirect_short_num, "+
                "t.direct_long_num "+
                "from (" +
                "select m.lrt_id,m.product_name,m.is_recomend,m.click_day,m.info_type,cast(sum(m.pv) as string) as pv," +
                "cast(sum(m.policy_num) as string) as deal_num, " +
                "cast(sum(m.total_premium) as string) as total_premium, " +
                "cast(sum(m.short_insurance_num) as string) as short_insurance_num, " +
                "cast(sum(m.long_insurance_num) as string) as long_insurance_num, " +
                "cast(sum(m.direct_num) as string) as direct_num, " +
                "cast(sum(m.undirect_num) as string) as undirect_num, " +
                "cast(sum(m.direct_short_num) as string) as direct_short_num, " +
                "cast(sum(m.undirect_short_num) as string) as undirect_short_num, " +
                "cast(sum(m.direct_long_num) as string) as direct_long_num " +
                " from "+tableName+" m group by m.lrt_id,m.product_name,m.click_day,m.is_recomend,m.info_type" +
                ") t";
        return DataFrameUtil.getDataFrame(sqlContext, sql, destName);
    }
    private DataFrame getAllEarliestAsResult(HiveContext sqlContext,String tableName1,String tableName2,String tableName3,String destName){
        String sql="select user_id,member_id,open_id,customer_id,lrt_id,product_name,lia_policyno,lia_accepttime,visit_time,is_recomend,insuranceType,premium,info_type,from_id from "+tableName1 +
                " union all " +
                " select user_id,member_id,open_id,customer_id,lrt_id,product_name,lia_policyno,lia_accepttime,visit_time,is_recomend,insuranceType,premium,info_type,from_id from "+tableName2 +
                " union all " +
                " select user_id,member_id,open_id,customer_id,lrt_id,product_name,lia_policyno,lia_accepttime,visit_time,is_recomend,insuranceType,premium,info_type,from_id from "+tableName3;
        return DataFrameUtil.getDataFrame(sqlContext, sql, destName);
    }
    /**
     * 详细表计算
     * @param sqlContext
     * @return
     */
    public DataFrame getDetailDF(HiveContext sqlContext){
        //-------微信端推荐页面浏览数据 info_type=0
        DataFrame dfwx=getRecommendWxPageVisit(sqlContext);
//        saveAsParquet(dfwx,"/user/tkonline/taikangtrack_test/data/recomend_wx");
        //微信链接大健康表
        DataFrame dfwx1= getOpenIdAndUserID(sqlContext,"RecommendWxPageVisit","RecommendUserWxPageVisit","是");
//        saveAsParquet(dfwx1,"/user/tkonline/taikangtrack_test/data/recomend_wx1da");

        //-------wap端推荐页面浏览数据 info_type=1
        DataFrame dfwap=getRecommenWapPageVisit(sqlContext);
//        saveAsParquet(dfwap,"/user/tkonline/taikangtrack_test/data/recomend_wap");
        //-------pc端推荐页面浏览数据 info_type=2
        DataFrame dfpc=getRecommendPcPageVisit(sqlContext);
//        saveAsParquet(dfpc,"/user/tkonline/taikangtrack_test/data/recomend_pc");

        // WAP和pc端数据合并
        getMergePcAndWapPageVisit(sqlContext,"RecommendWapPageVisit","RecommendPcPageVisit","RecommendWapAndPcPageVisit");
        //用WAP 和pc端数据合并的数据关联大健康表
        getPcIdAndWapIdAndUserID(sqlContext,"RecommendWapAndPcPageVisit","RecommendUserWapAndPcPageVisit","是");

        //合并三端的数据
        DataFrame dfRemmendAll=getRecommendAllUserID(sqlContext,"RecommendUserWxPageVisit","RecommendUserWapAndPcPageVisit","RecommendCommonUserPageVisit");
//        saveAsParquet(dfRemmendAll,"/user/tkonline/taikangtrack_test/data/recomend_all");

        //获取唯一的用户信息表
        DataFrame dfb=getLifeInsureCustomerId(sqlContext);
//        saveAsParquet(dfb,"/user/tkonline/taikangtrack_test/data/recomden_evalute_vpbbb");
        //寿险表
        DataFrame df0=getUserVaildPolicy(sqlContext);
//        saveAsParquet(df0,"/user/tkonline/taikangtrack_test/data/recomden_evalute_vp");
        //寿险表的险种
        DataFrame dftype=getPolicyinstranceType(sqlContext);

        //合并财险和寿险保单信息
        DataFrame sumP=getSumUserVaildPolicy(sqlContext);
//        saveAsParquet(sumP,"/user/tkonline/taikangtrack_test/data/recomden_evalute_SumUserVaildPolicy");

        //全部浏览记录关联全部保单和去重
        DataFrame summeryDF = getSummeryUserPolicyConnectBrowser(sqlContext, "SumUserVaildPolicy", "RecommendCommonUserPageVisit", "UserPolicyConnectBrowser_all_temp");
//        saveAsParquet(summeryDF,"/user/tkonline/taikangtrack_test/data/UserPolicyConnectBrowser_all_temp");
        DataFrame summeryDF1 = getEarliestAsResult(sqlContext, "UserPolicyConnectBrowser_all_temp", "EarliestPolicyResult_early_temp", "是");
//        saveAsParquet(summeryDF1,"/user/tkonline/taikangtrack_test/data/EarliestPolicyResult_early_temp");
        DataFrame summeryDF2 = getPolicyAndTimeAsResult(sqlContext,"EarliestPolicyResult_early_temp","EarliestPolicyResult_dirction_temp");
//                saveAsParquet(summeryDF2,"/user/tkonline/taikangtrack_test/data/EarliestPolicyResult_dirction_temp");
        DataFrame summeryDF277 =getDistictPolicyAsResult(sqlContext,"EarliestPolicyResult_dirction_temp","EarliestPolicyResult_early_temp","EarliestPolicyResult");
//                saveAsParquet(summeryDF277,"/user/tkonline/taikangtrack_test/data/EarliestPolicyResult");

        //------非推荐页面微信浏览数据--------------
        getNonRecommendWxPageVisit(sqlContext);
        getOpenIdAndUserID(sqlContext,"NonRecommendWxPageVisit","NonRecommendWxUserPageVisit","否");

        // ------非推荐页面wap浏览数据--------------
        //所有的wap详情页
        DataFrame dfAllWap=getWapUserBrowseInfos(sqlContext);
//       saveAsParquet(dfAllWap,"/user/tkonline/taikangtrack_test/data/all_wap");
        //推荐页面的user_id and lrtid
        DataFrame dfCommenWap=getCommenWapPageVisit(sqlContext);
//       saveAsParquet(dfCommenWap,"/user/tkonline/taikangtrack_test/data/dfCommenWap");
        //非推荐页面wap浏览数据
        DataFrame dfNoWap=getNonRecommendWapPageVisit(sqlContext);
//       saveAsParquet(dfNoWap,"/user/tkonline/taikangtrack_test/data/Non_recomend_wap");

        // ------非推荐页面pc浏览数据--------------
        //所有的pc详情页，插码改版后，处理PC端浏览记录
        DataFrame dfnewAllpc=getPcNewUserBrowseInfos(sqlContext);
//        saveAsParquet(dfnewAllpc,"/user/tkonline/taikangtrack_test/data/all_nrepc");
        //插码改版前，处理PC端浏览记录
        DataFrame dfoldAllpc=getPcOldUserBrowseInfos(sqlContext);
//        saveAsParquet(dfoldAllpc,"/user/tkonline/taikangtrack_test/data/all_oldpc");
        //合并PC端浏览记录
        DataFrame dfAllpc=getPcUserBrowseInfos(sqlContext);
//       saveAsParquet(dfAllpc,"/user/tkonline/taikangtrack_test/data/all_pc");
        //推荐页面的user_id lrt_id
        DataFrame dfCommenPc=getCommenPcPageVisit(sqlContext);
//       saveAsParquet(dfCommenPc,"/user/tkonline/taikangtrack_test/data/dfCommenPc");
        //非推荐页面pc浏览数据
        DataFrame dfNopc=getNonRecommendPcPageVisit(sqlContext);
//       saveAsParquet(dfNopc,"/user/tkonline/taikangtrack_test/data/Non_recomend_pc");

        //合并非推荐页面wap和pc浏览数据--------------
        getMergePcAndWapPageVisit(sqlContext,"NonRecommendWapPageVisit","NonRecommendPcPageVisit","NonRecommendWapAndPcPageVisit");
        //用合并非推荐页面wap和pc浏览数据和大健康表相连--------------
        getPcIdAndWapIdAndUserID(sqlContext,"NonRecommendWapAndPcPageVisit","NonRecommendUserWapAndPcPageVisit","否");
        //合并非推荐三端的数据
        DataFrame dfNonRemmendAll=getRecommendAllUserID(sqlContext,"NonRecommendWxUserPageVisit","NonRecommendUserWapAndPcPageVisit","NonRecommendCommonUserPageVisit");
//       saveAsParquet(dfNonRemmendAll,"/user/tkonline/taikangtrack_test/data/non_recomend_all");

        //找到推荐保单
        getUsedPolicyNo(sqlContext,"EarliestPolicyResult");
        //总表单减去推荐保单，剩下的就是非推荐的保单
        getNonUsedPolicyNo(sqlContext);

        //没加from_id时，非推荐的保单和去重
        DataFrame summeryDF23 = getSummeryUserPolicyConnectBrowser(sqlContext, "NonUsedPolicyNo", "NonRecommendCommonUserPageVisit", "NonUserPolicyConnectBrowser_all_temp");
//        saveAsParquet(summeryDF23,"/user/tkonline/taikangtrack_test/data/NonUserPolicyConnectBrowser_all_temp");
        DataFrame summeryDF3 = getEarliestAsResult(sqlContext, "NonUserPolicyConnectBrowser_all_temp", "NonEarliestPolicyResult_early_temp", "否");
//        saveAsParquet(summeryDF3,"/user/tkonline/taikangtrack_test/data/NonEarliestPolicyResult_early_temp");
        DataFrame summeryDF33=getPolicyAndTimeAsResult(sqlContext,"NonEarliestPolicyResult_early_temp","NonEarliestPolicyResult_dirction_temp");
//                saveAsParquet(summeryDF33,"/user/tkonline/taikangtrack_test/data/NonEarliestPolicyResult_dirction_temp");
        DataFrame summeryDF335= getDistictPolicyAsResult(sqlContext,"NonEarliestPolicyResult_dirction_temp","NonEarliestPolicyResult_early_temp","NonEarliestPolicyResult");
//                saveAsParquet(summeryDF335,"/user/tkonline/taikangtrack_test/data/NonEarliestPolicyResult");


//      合并两个页面浏览记录
        DataFrame m1=MergeRecomendAndNonRecomendPB(sqlContext);
//        saveAsParquet(m1,"/user/tkonline/taikangtrack_test/data/recomden_evalute_m1");
        //合并两个用户保单记录
        DataFrame m2=MergeRecomendAndNonRecomendUP(sqlContext);
//        saveAsParquet(m2,"/user/tkonline/taikangtrack_test/data/recomden_evalute_m2");

//        MergeRecomendAndNonRecomendUP_TEMP

        String sysDate = GetSysDate(0);
        //用from_id来区分平台
        DataFrame getRecomendAndNonRecomendUpByFromId = getRecomendAndNonRecomendUpByFromId(sqlContext);
        saveAsParquet(getRecomendAndNonRecomendUpByFromId,"/user/tkonline/taikangtrack_test/data/recomden_evalute_other_feiziyou");
        //非自有平台的from_id
        DataFrame NonOwner = getNonOwnerOtherPolicyData(sqlContext);
        saveAsParquet(NonOwner,"/user/tkonline/taikangtrack_test/data/recomend_evaluate_detail_nonowner-"+sysDate);

        //推荐和非推 用户行为对应保单表 合并后，把它转化为详情表
        DataFrame dfDetail=getUserBrowserPolicyDetail(sqlContext, "MergeRecomendAndNonRecomendUP", "UserBrowserPolicyDetail");
//        saveAsParquet(dfDetail,"/user/tkonline/taikangtrack_test/data/recomden_evalute_dfDetail");
        return dfDetail;
    }

    private DataFrame getRecomendAndNonRecomendUpByFromId(HiveContext sqlContext) {
        String sql="select other.user_id,other.member_id,other.open_id,other.customer_id,other.lrt_id,other.product_name,case when plat.platform='微信' then '0' when plat.platform='WAP' then '1' when plat.platform='PC' then '2' when plat.platform='APP' then '3'  else '4' end as info_type,other.lia_policyno,other.lia_accepttime,other.visit_time,other.is_recomend,other.insuranceType,other.premium,other.from_id \n" +
                " from MergeRecomendAndNonRecomendUP_TEMP other left join tknwdb.handle_client plat  on other.from_id = plat.from_id where plat.platform='微信' or plat.platform='WAP' or plat.platform='PC' or plat.platform='APP'";
        return DataFrameUtil.getDataFrame(sqlContext, sql, "MergeRecomendAndNonRecomendUP");
    }

    private DataFrame getNonOwnerOtherPolicyData(HiveContext sqlContext) {
        String sql="select other.user_id,other.member_id,other.open_id,other.customer_id,other.lrt_id,other.product_name,other.info_type,other.lia_policyno,cast(from_unixtime(cast(unix_timestamp(other.lia_accepttime) as bigint), 'yyyy-MM-dd HH:mm:ss') as  string) lia_accepttime,cast(from_unixtime(cast(other.visit_time/1000 as bigint), 'yyyy-MM-dd HH:mm:ss') as  string) visit_time,other.is_recomend,other.insuranceType,other.premium,other.from_id,find.name " +
                " from  tknwdb.handle_client find  inner join MergeRecomendAndNonRecomendUP other  on other.from_id = find.from_id where find.platform <> '微信' and find.platform <> 'WAP' and find.platform <> 'PC' and find.platform <> 'APP' ";
        return DataFrameUtil.getDataFrame(sqlContext, sql, "NonOwnerOtherPolicyData");
    }

    private DataFrame getZiYouOtherPolicyData(HiveContext sqlContext,String srcTable,String descTable) {
        String sql="select other.user_id,other.member_id,other.open_id,other.customer_id,other.lrt_id,other.product_name,case when plat.platform='微信' then '0' when plat.platform='WAP' then '1' when plat.platform='PC' then '2' when plat.platform='APP' then '3'  else '4' end as info_type,other.lia_policyno,other.lia_accepttime,other.visit_time,other.is_recomend,other.insuranceType,other.premium,other.from_id \n" +
                " from "+srcTable+" other left join tknwdb.handle_client plat  on other.from_id = plat.from_id";
        return DataFrameUtil.getDataFrame(sqlContext, sql, srcTable);
    }

    private DataFrame MergeOthereRecomendAndNonRecomendUP(HiveContext sqlContext) {
        String sql="select  a.user_id,a.member_id,a.open_id,a.customer_id,a.lrt_id,a.product_name,a.info_type,a.lia_policyno,a.lia_accepttime,a.visit_time,a.is_recomend,a.insuranceType,a.premium,a.from_id from allFindRecomendPolicyByFromId a\n" +
                    "union all\n" +
                    "select  a.user_id,a.member_id,a.open_id,a.customer_id,a.lrt_id,a.product_name,a.info_type,a.lia_policyno,a.lia_accepttime,a.visit_time,a.is_recomend,a.insuranceType,a.premium,a.from_id from allFindNonRecomendPolicyByFromId a";
        return DataFrameUtil.getDataFrame(sqlContext, sql, "mergeOthereRecomendAndNonRecomendUP");
    }



    private DataFrame getPcUserBrowseInfos(HiveContext sqlContext) {
        String sql="select terminal_id,user_id,subtype,time,event,lrt_id,product_name,info_type from pc_oldUserBrowseInfos\n" +
        "union all\n" +
        "select terminal_id,user_id,subtype,time,event,lrt_id,product_name,info_type from pc_newUserBrowseInfos";
        return DataFrameUtil.getDataFrame(sqlContext, sql, "pc_UserBrowseInfos");
    }

    private DataFrame getPcOldUserBrowseInfos(HiveContext sqlContext) {
        String sql="select t.terminal_id,\n" +
                "       t.user_id,\n" +
                "       t.subtype,\n" +
                "       t.time,\n" +
                "       t.event,\n" +
                "       p.product_num as lrt_id,\n" +
                "       t.product_name,\n" +
                "       t.info_type from \n" +
                " (select tul.terminal_id,\n" +
                "               ftm.user_id,\n" +
                "               tul.subtype,\n" +
                "               tul.time,\n" +
                "               tul.event,\n" +
                "               regexp_extract(tul.label, 'productID:\"([0-9]+)\"') as product_id,\n" +
                "               regexp_extract(tul.label, '(?<=productName:\")[^\",]*', 0) as product_name,\n" +
                "               '2' as info_type\n" +
                "          from uba_log_event tul\n" +
                "         inner join f_terminal_map ftm\n" +
                "            on tul.terminal_id = ftm.terminal_id\n" +
                "           and tul.user_id = ftm.user_id\n" +
                "         where tul.event = '商品详情'\n" +
                "           and tul.app_Type = 'mall'\n" +
                "           and tul.app_Id = 'mall001') t inner join tknwdb.product p\n" +
                "        on p.id = t.product_id";

        return DataFrameUtil.getDataFrame(sqlContext, sql, "pc_oldUserBrowseInfos");
    }


    private DataFrame getSummeryUserPolicyConnectBrowser(HiveContext sqlContext, String tableName1, String tableName, String destName) {
        String sql="select rv.user_id,rv.member_id,rv.open_id,rv.lrt_id,rv.product_name,rv.time,rv.info_type," +
                "up.lia_policyno,up.lia_accepttime,up.insuranceType,up.premium,up.customer_id,up.from_id " +
                " from "+tableName1+" up  inner join "+tableName+" rv on up.user_id=rv.user_id and " +
                "  up.lrt_id=rv.lrt_id where unix_timestamp(up.lia_accepttime) >= " +
                "cast(rv.time/1000 as bigint)";
        return DataFrameUtil.getDataFrame(sqlContext, sql, destName);
    }

    private DataFrame getAllOtherPolicyByFromId(HiveContext sqlContext, String desTable) {
        String sql="select a.user_id,a.member_id,a.open_id,a.customer_id,a.lrt_id,a.product_name,a.info_type,a.lia_policyno,a.lia_accepttime,a.visit_time,a.is_recomend,a.insuranceType,a.premium,a.from_id " +
                " from SummeryEarliestPolicyResult a left join EarliestPolicyResult b on a.lia_policyno = b.lia_policyno where b.lia_policyno is null";
        return DataFrameUtil.getDataFrame(sqlContext, sql, desTable);
    }
    private DataFrame getAllOtherNonPolicyByFromId(HiveContext sqlContext, String desTable) {
        String sql="select a.user_id,a.member_id,a.open_id,a.customer_id,a.lrt_id,a.product_name,a.info_type,a.lia_policyno,a.lia_accepttime,a.visit_time,a.is_recomend,a.insuranceType,a.premium,a.from_id " +
                " from SummeryNonEarliestPolicyResult a left join NonEarliestPolicyResult b on a.lia_policyno = b.lia_policyno where b.lia_policyno is null";
        return DataFrameUtil.getDataFrame(sqlContext, sql, desTable);
    }

    private void getPlatformByFromId(HiveContext sqlContext, String srcTable, String platFromName, String descTable) {
        String sql="select tab.user_id,\n" +
                "       tab.member_id,\n" +
                "       tab.customer_id,\n" +
                "       tab.open_id,\n" +
                "       tab.LRT_ID,\n" +
                "       tab.lia_policyno,\n" +
                "       tab.lia_accepttime,\n" +
                "       tab.insuranceType,\n" +
                "       tab.premium,\n" +
                "       tab.from_id\n" +
                "  from "+srcTable+" tab\n" +
                " inner join  tknwdb.handle_client plat\n" +
                "              on plat.platform like '%"+platFromName+"%' and  tab.from_id = plat.from_id";
        DataFrameUtil.getDataFrame(sqlContext, sql, descTable);
    }


    /**
     * 根据详情表聚合出结果
     * @param sqlContext
     * @return
     */
    public DataFrame MergeResult(HiveContext sqlContext){
        getPolicyUserIdGroup(sqlContext, "UserBrowserPolicyDetail", "MerPolicyOpenIdGroup");
        getBrowserUserIdGroup(sqlContext,"MergeRecomendAndNonRecomendPB","MerBrowserOpenIdGroup");
        DataFrame dfM=mergeAndGroupByProduct(sqlContext,"MerBrowserOpenIdGroup","MerPolicyOpenIdGroup","mergeAndGroupByProduct");
//        saveAsParquet(dfM,"/user/tkonline/taikangtrack_test/data/recomden_evalute_final_mer");
        DataFrame dffinal=getFinalGroupResult(sqlContext,"mergeAndGroupByProduct","FinalGroupResult");
        return dffinal;
    }

    private DataFrame MergeRecomendAndNonRecomendPB(HiveContext sqlContext){

        String sql="select terminal_id,user_id,member_id,open_id," +
                "lrt_id,subtype,time,event,product_name,is_recomend,info_type from RecommendCommonUserPageVisit " +
                " union all "+
                "select terminal_id,user_id,member_id,open_id," +
                "lrt_id,subtype,time,event,product_name,is_recomend,info_type from NonRecommendCommonUserPageVisit ";
        return DataFrameUtil.getDataFrame(sqlContext, sql, "MergeRecomendAndNonRecomendPB", DataFrameUtil.CACHETABLE_EAGER);

    }
    private DataFrame MergeRecomendAndNonRecomendUP(HiveContext sqlContext){
        String sql="select user_id,member_id,open_id,customer_id,lrt_id,product_name,lia_policyno,lia_accepttime,visit_time,is_recomend,info_type,insuranceType,premium,from_id " +
                " from EarliestPolicyResult " +
                " union all " +
                " select user_id,member_id,open_id,customer_id,lrt_id,product_name,lia_policyno,lia_accepttime,visit_time,is_recomend,info_type,insuranceType,premium,from_id  " +
                " from NonEarliestPolicyResult ";
        return DataFrameUtil.getDataFrame(sqlContext, sql, "MergeRecomendAndNonRecomendUP_TEMP", DataFrameUtil.CACHETABLE_EAGER);
    }


    private DataFrame getNonRecommendWxPageVisit(HiveContext sqlContext){
        String caseWhenPid=getCaseProductId(productKvMap,"subtype");
        String caseWhenPName=getCaseProductName(productKvMap, "subtype");
        String caseWhenPIn=getProductIn(productKvMap, "subtype",true);
        String sql= "select terminal_id,user_id,subtype,time,event, " +
//                "case when subtype='体检保' then 'S20160091'  " +
//                " when subtype='少儿重疾' then 'S20160159'  " +
//                " when subtype='成人重疾' then 'S20160158'   " +
//                " when subtype='微互助求关爱' then '241'   " +
//                " when subtype='综合意外' then '239'   " +
//                " when subtype='老年癌症医疗保险' then 'S20160266'   " +
//                " when subtype='定期寿险' then '281'  " +
//                " when subtype='e享健康返本型重疾险' then '298'   " +
//                " when subtype='住院保' then 'S20160203'  " +
//                " when subtype='E理财B投连险' then '522'  " +
//                " when subtype='门急诊住院报销' then '282'  " +
//                " when subtype='万里无忧B款' then '297'  " +
//                " when subtype='短期旅行意外' then '1103'  " +
//                " when subtype='健康1+1返本型重疾险' then '290'  " +
//                " when subtype='悦享中华高端医疗费用报销' then '249' else ''  " +
//                " end as lrt_id,  " +
                caseWhenPid+","+
                caseWhenPName+","+
//                "case when subtype='体检保' then '体检保'  " +
//                " when subtype='少儿重疾' then '少儿重疾'  " +
//                " when subtype='成人重疾' then '成人重疾'   " +
//                " when subtype='微互助求关爱' then '微互助求关爱'   " +
//                " when subtype='综合意外' then '综合意外'   " +
//                " when subtype='老年癌症医疗保险' then '老年癌症医疗保险'   " +
//                " when subtype='定期寿险' then '定期寿险'  " +
//                " when subtype='e享健康返本型重疾险' then 'e享健康返本型重疾险'   " +
//                " when subtype='住院保' then '住院保'  " +
//                " when subtype='E理财B投连险' then 'E理财B投连险'  " +
//                " when subtype='门急诊住院报销' then '门急诊住院报销'  " +
//                " when subtype='万里无忧B款' then '万里无忧B款'  " +
//                " when subtype='短期旅行意外' then '短期旅行意外'  " +
//                " when subtype='健康1+1返本型重疾险' then '健康1+1返本型重疾险'  " +
//                " when subtype='悦享中华高端医疗费用报销' then '悦享中华高端医疗费用报销' else ''  " +
//                " end as product_name  " +
                  " '0' as info_type  " +
                " from uba_log_event where event in ('意外险','健康险','理财','财产险') and user_id is not null and user_id <>'' " +
                "and subtype is not null " +
                caseWhenPIn;
//                "and subtype in " +
//                "('体检保','少儿重疾','微互助求关爱','成人重疾','综合意外','老年癌症医疗保险','定期寿险'," +
//                "'e享健康返本型重疾险','住院保','E理财B投连险','门急诊住院报销','万里无忧B款','短期旅行意外'," +
//                "'健康1+1返本型重疾险','悦享中华高端医疗费用报销')  ";
        return DataFrameUtil.getDataFrame(sqlContext, sql, "NonRecommendWxPageVisit");

    }

//    private DataFrame getUnionResult(HiveContext sqlContext){
//        String sql="select * from (select " +
//                "lrt_id ," +
//                "product_name ," +
//                "'是' as is_recomend,"+
//                "click_day ," +
//                "pv ," +
//                "deal_num  " +
//                "from FinalGroupResult" +
//                " union all " +
//                "select " +
//                "lrt_id ," +
//                "product_name ," +
//                "'否' as is_recomend,"+
//                "click_day ," +
//                "pv ," +
//                "deal_num  " +
//                " from NonFinalGroupResult) t order by t.lrt_id";
//
//        return DataFrameUtil.getDataFrame(sqlContext, sql, "UnionResult");
//    }

    /**
     * 关联财险保单表和产品配置表
     * 得到保单信息，其中产品编号为销售方案编号
     * @param sqlContext
     * @return
     */
    private DataFrame getPropertyInsurancePolicy(HiveContext sqlContext){
        String sql="select p.policyno,p.sumgrosspremium,p.operatedate,'财险' as insuranceType,c.splancode,p.surveyind as from_id from " +
                " tkubdb.gschannelplancode c inner join GUPOLICYCOPYMAIN p on c.dplancode=p.flowid where c.dplancode is not null and " +
                " p.flowid is not null group by p.policyno,p.sumgrosspremium,p.operatedate,c.splancode,p.surveyind";

        return DataFrameUtil.getDataFrame(sqlContext, sql, "PropertyInsurancePolicy");
    }
    /**
     * 关联财险用户表和寿险投保人表，得到用户信息。(带有财险保单）
     * @param sqlContext
     * @return
     */
    private DataFrame getCommonUser(HiveContext sqlContext) {
        String sql = "select a.user_id,a.member_id,a.customer_id,a.open_id,a.cid_number,a.insuredname,a.identifynumber,a.insuredflag,a.policyno " +
                "from (" +
                "select u.user_id,u.member_id,u.customer_id,u.open_id,u.cid_number,p.insuredname,p.identifynumber," +
                "p.insuredflag,p.policyno from " +
                "(select  identifynumber,insuredname,insuredflag,policyno from GUPOLICYRELATEDPARTY where identifynumber is not null and insuredname is not null " +
                " group by identifynumber,insuredname,insuredflag,policyno ) p " +
                " inner join " +
                " (select  user_id,member_id,customer_id,open_id,cid_number,name from FACT_USERINFO group by " +
                "user_id,member_id,customer_id,open_id,cid_number,name ) u " +
                " on p.identifynumber=u.cid_number and p.insuredname=u.name ) a " +
                "group by a.user_id,a.member_id,a.customer_id,a.open_id,a.cid_number,a.insuredname,a.identifynumber,a.insuredflag,a.policyno ";

        return DataFrameUtil.getDataFrame(sqlContext, sql, "CommonUser");
    }
    /**
     * 关联财险投保人表和PropertyInsurancePolicy 得到投保人的各种id及姓名和证件号码及保单信息
     * @param sqlContext
     * @return
     */
    private DataFrame getPropertyInsuranceUserPolicy(HiveContext sqlContext){
        String sql="select a.user_id,a.member_id,a.customer_id,a.open_id,a.cid_number,a.identifynumber," +
                "a.insuredname,a.policyno,a.premium,a.operatedate,a.splancode,a.insuranceType,a.from_id " +
                "from (select t.user_id,t.member_id,t.customer_id,t.open_id,t.cid_number,t.identifynumber,t.insuredname," +
                "gm.policyno,gm.sumgrosspremium as premium,gm.operatedate," +
                "gm.splancode,gm.insuranceType,gm.from_id " +
                "from CommonUser t " +
                "inner join PropertyInsurancePolicy gm on t.policyno = gm.policyno " +
                "where t.insuredflag = '1' and t.identifynumber is not null and t.insuredname is not null and " +
                "gm.sumgrosspremium <>'0' and gm.sumgrosspremium <>0 ) a  " +
                "group by a.user_id,a.member_id,a.customer_id,a.open_id,a.cid_number,a.identifynumber,a.insuredname," +
                "a.policyno,a.premium,a.operatedate,a.splancode,a.insuranceType,a.from_id";

        return DataFrameUtil.getDataFrame(sqlContext, sql, "PropertyInsuranceUserPolicy");
    }

    /**
     * 合并用户拥有的 财险和寿险 保单
     * 注意： 财险保单 中有一部分保单是从寿险表中导进来的，需要统一标准（两边取的承保时间得是一个时间，是寿险传
     * 给财险的时间），然后去重
     * @param sqlContext
     * @return
     */
    private DataFrame getSumUserVaildPolicy(HiveContext sqlContext){

        String sql="select  user_id,member_id,customer_id,open_id,LRT_ID,lia_policyno," +
                "lia_accepttime,insuranceType,premium,from_id " +
                " from (select  user_id,member_id,customer_id,  " +
                "open_id,  " +
                "LRT_ID,  " +
                "lia_policyno,  " +
                "lia_accepttime," +
                "insuranceType,premium,from_id  from vaildPolicyInstanceType " +
                " union all " +
                "select user_id,member_id,customer_id,open_id," +
                "splancode as lrt_id,policyno as lia_policyno," +
                "operatedate as lia_accepttime,insuranceType,premium,from_id " +
                "from PropertyInsuranceUserPolicy ) t group by  user_id,member_id,customer_id,open_id,LRT_ID,lia_policyno," +
                "lia_accepttime,insuranceType,premium,from_id ";
        return DataFrameUtil.getDataFrame(sqlContext, sql, "SumUserVaildPolicy", DataFrameUtil.CACHETABLE_EAGER);
    }

    /**
     * 推荐和非推 用户行为对应保单表 合并后，把它转化为详情表
     * @param sqlContext
     * @param srcTable
     * @param desttableName
     * @return
     */
    private DataFrame getUserBrowserPolicyDetail(HiveContext sqlContext,String srcTable,String desttableName){

        String sql="select user_id,member_id,open_id,customer_id,lrt_id,product_name,lia_policyno,info_type," +
                "cast(from_unixtime(cast(unix_timestamp(lia_accepttime) as bigint), 'yyyy-MM-dd HH:mm:ss') as  string) lia_accepttime, " +
                "cast(from_unixtime(cast(visit_time/1000 as bigint), 'yyyy-MM-dd HH:mm:ss') as  string) visit_time," +
                "is_recomend,premium, " +
                "case when insuranceType='短险' then '1' else '0' end as short_insurance, " +
                "case when insuranceType='长险' then '1' else '0' end as long_insurance, " +
                "case when insuranceType='财险' then '1' else '0' end as preser_insurance, " +
                "case when insuranceType='理财险' then '1' else '0' end as finance_insurance, " +
                "case when netpolicyno=lia_policyno then '1' else '0' end as undirect_flag, " +
                "case when netpolicyno=lia_policyno then '0' else '1' end as direct_flag  " +
                " from ( " +
                "select ear.user_id," +
                "CASE  WHEN split(ear.open_id,'_ID')[0]='OPEN' THEN   " +
                "'' " +
                "ELSE  " +
                "ear.open_id  " +
                "END AS open_id, " +
                "CASE  " +
                "WHEN split(ear.member_id,'_ID')[0]='MEMBER' THEN  " +
                "''  " +
                "ELSE   " +
                "ear.member_id  " +
                "END AS member_id, " +
                "ear.customer_id,ear.lrt_id,ear.product_name,ear.lia_policyno,ear.lia_accepttime,ear.info_type ," +
                "ear.visit_time,ear.is_recomend,ear.insuranceType,ear.premium,t.netpolicyno  " +
                " from "+srcTable+" ear left join ( " +
                "select lia_policyno as netpolicyno from tknwdb.sales_trade group by lia_policyno) t on t.netpolicyno=ear.lia_policyno " +
                ") b";
        return DataFrameUtil.getDataFrame(sqlContext, sql, desttableName);
    }
    
    /**
     * 推荐监控的  详细情况
     */
    public DataFrame getRecomendWithoutAlgoEvalDetail(HiveContext sqlContext){
        //将大健康表空id转化,防止倾斜
        getFactHealthcreditscoreresultNotNullDF(sqlContext);

        //计算出财险用户保单信息
        DataFrame df1= getPropertyInsurancePolicy(sqlContext);
//        saveAsParquet(df1, "/user/tkonline/taikangtrack_test/data/recomden_evalute_propertyinsurancepolicy");
        DataFrame df2= getCommonUser(sqlContext);
//        saveAsParquet(df2, "/user/tkonline/taikangtrack_test/data/recomden_evalute_CommonUser");
        DataFrame df3= getPropertyInsuranceUserPolicy(sqlContext);
//        saveAsParquet(df3, "/user/tkonline/taikangtrack_test/data/recomden_evalute_PropertyInsuranceUserPolicy");

        return getDetailDF(sqlContext);
    }








    public void saveAsParquet(DataFrame df, String path) {
        TK_DataFormatConvertUtil.deletePath(path);
        df.saveAsParquetFile(path);
    }

    private String getCaseProductId(String kvStr,String column){
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
        resultStr+=" else '' end as lrt_id ";
        return resultStr;
    }
    private String getCaseProductName(String kvStr,String column){
        String[] strs=kvStr.split(",");
        Map<String,String> map=new HashMap<String,String>();

        if(strs!=null&&strs.length>0){
            for (String s : strs) {

                String[] ss=s.split(":");
                if(ss.length>=2){
                    String s1=s.split(":")[0];
//                    String s2=s.split(":")[1];
                    map.put(s1,s1);
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
        resultStr+=" else '' end as product_name ";
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
    public static String GetSysDate(int day) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, day);
        String dateStr = new SimpleDateFormat("yyyyMMdd").format(cal.getTime());
        return dateStr;
    }
}
