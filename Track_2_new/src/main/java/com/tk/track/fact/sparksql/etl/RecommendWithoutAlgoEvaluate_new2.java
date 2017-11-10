package com.tk.track.fact.sparksql.etl;

import com.tk.track.fact.sparksql.main.App;
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
public class RecommendWithoutAlgoEvaluate_new2 implements Serializable {
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
//                 + "          case when tul.event='shop首页推荐' then '4' else '2' end as info_type "
                 + "          '2' as info_type "
                 + "         from uba_log_event  tul "
                 + "      inner join f_terminal_map ftm "
                 + "            on tul.terminal_id=ftm.terminal_id and tul.user_id=ftm.user_id "
                 + "	        where (tul.event = '首页推荐' or tul.event='shop首页推荐' )"
                 + "	         and tul.app_Type = 'webSite' "
                 + "	        and tul.app_Id = 'webSite003' ";
    	 return DataFrameUtil.getDataFrame(sqlContext, sql, "RecommendPcPageVisit");
    }

    
    private DataFrame getRecommendAppPageVisit(HiveContext sqlContext){

   	 String sql = "select tul.terminal_id, "
                + "           ftm.user_id, tul.subtype, tul.time, tul.event, "
                + "      case when regexp_extract(tul.label,'productId:\"([0-9]+)\"') is not null and  regexp_extract(tul.label,'productId:\"([0-9]+)\"') <> '' then regexp_extract(tul.label,'productId:\"([0-9]+)\"')"
                + "           else regexp_extract(tul.label,'productId:\"(S[0-9]+)\"')"
                +"        end as  lrt_id ,   "
                + "           regexp_extract(tul.label,'(?<=productName:\")[^\",]*',0) as product_name, "
//                + "      case when tul.currenturl like '%fromType=home%' then '3' "
//                + "           when tul.currenturl like '%fromType=search%' then '5' "
//                + "           when tul.currenturl like '%fromType=searchNoResult%' then '6' end as info_type "
                + "          '3' as info_type "
                + "         from uba_log_event  tul "
                + "      inner join f_terminal_map ftm "
                + "            on tul.terminal_id=ftm.terminal_id  and tul.app_type=ftm.app_type and tul.app_id=ftm.app_id"
                + "	        where tul.app_type ='H5' and tul.event<> 'page.load' and tul.event<> 'page.unload' "
                + "           and (tul.currenturl like '%fromType=home%' "
                + "	          or tul.currenturl like '%fromType=search%' "
                + "	          or  tul.currenturl like '%fromType=searchNoResult%')";
   	 return DataFrameUtil.getDataFrame(sqlContext, sql, "RecommendAppPageVisit");
   }

    /**
     * WAP和pc端数据合并
     * @param sqlContext
     * @return
     */
    private DataFrame getMergePcAndWapPageVisit(HiveContext sqlContext,String tableName1,String tableName2,String tableName3,String destTableName){
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
    			 +"  from "+tableName2 
    	         +" union all "
		         +" select terminal_id, "
		         +"       user_id, "
		         +"       subtype, "
		         +"       time, "
		         +"       event, "
		         +"       lrt_id, "
		         +"       product_name, "
		         +"       info_type "
		         +"  from "+tableName3 ;
    	 return DataFrameUtil.getDataFrame(sqlContext, sql, destTableName);
    }


    /**
     * 当源表中user_id 为open_id 时关联fact_healthcreditscoreresult 取通用的 userid，原userId作为open_id
     * @param sqlContext
     * @param tableName
     * @return
     */
    private DataFrame getOpenIdAndUserID(HiveContext sqlContext,String tableName,String destTableName,String isRecomend) {
        String sql = "SELECT /*+MAPJOIN(orgtable)*/ orgtable.terminal_id,h.user_id,h.member_id,h.customer_id,h.cid_number,h.name,orgtable.user_id open_id," +
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

    
    private DataFrame getNonRecommendAppPageVisit(HiveContext sqlContext){
    	 String sql = "select tul.terminal_id, "
                 + "           ftm.user_id, tul.subtype, tul.time, tul.event, "
                 + "      case when regexp_extract(tul.label,'productId:\"([0-9]+)\"') is not null and  regexp_extract(tul.label,'productId:\"([0-9]+)\"') <> '' then regexp_extract(tul.label,'productId:\"([0-9]+)\"')"
                 + "           else regexp_extract(tul.label,'productId:\"(S[0-9]+)\"')"
                 +"        end as  lrt_id ,   "
                 + "           regexp_extract(tul.label,'(?<=productName:\")[^\",]*',0) as product_name, "
                 + "      '3' as info_type "
                 + "         from uba_log_event  tul "
                 + "      inner join f_terminal_map ftm "
                 + "            on tul.terminal_id=ftm.terminal_id  and tul.app_type=ftm.app_type and tul.app_id=ftm.app_id"
                 + "	        where tul.app_type ='H5' and tul.event<> 'page.load' and tul.event<> 'page.unload' "
                 + "           and tul.currenturl not like '%fromType%' ";
    	return DataFrameUtil.getDataFrame(sqlContext, sql, "NonRecommendAppPageVisit");
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
                + "      CASE "
                + "        WHEN CUSTOMER_ID = '' OR CUSTOMER_ID is null "
                + "          THEN  genrandom('CUSTOMER_ID_') "
                + "      ELSE CUSTOMER_ID END AS CUSTOMER_ID, "
                + "      CASE "
                + "        WHEN CID_NUMBER = '' OR CID_NUMBER is null "
                + "          THEN  genrandom('CID_NUMBER_') "
                + "      ELSE CID_NUMBER END AS CID_NUMBER, "
                + "      CASE "
                + "        WHEN NAME = '' OR NAME is null "
                + "          THEN  genrandom('NAME') "
                + "      ELSE NAME END AS NAME, "
                + "	USER_ID "
                + "	FROM fact_healthcreditscoreresult where (CUSTOMER_ID is not null and CUSTOMER_ID <> '') "
                + " or (CID_NUMBER is not null and CID_NUMBER <> '') ";
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
        String sql = "SELECT /*+MAPJOIN(orgtable)*/ orgtable.terminal_id,h.user_id,h.customer_id,h.cid_number,h.name,orgtable.user_id member_id,h.open_id," +
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
        		+"   customer_id,cid_number,name,  "
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
        		+"   customer_id,cid_number,name,  "
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
    
    private DataFrame getRecommenVisitNoCustomerId(HiveContext sqlContext,String tableName,String destTableName ) {
        String sql = " select terminal_id, "
        		+"       user_id, "
        		+"       member_id, "
        		+"   customer_id,cid_number,name,  "
        		+"       open_id, "
        		+"       subtype, "
        		+"       time, "
        		+"       event, "
        		+"       lrt_id, "
        		+"       product_name, "
        		+"       info_type, "
        		+"       is_recomend "
        		+"   from "+tableName
        		+"     where customer_id is not null and customer_id <> '' ";

        return DataFrameUtil.getDataFrame(sqlContext, sql, destTableName, DataFrameUtil.CACHETABLE_PARQUET);
    }
    
   
    private DataFrame getLifeDistinctCustomerId(HiveContext sqlContext,String tableName,String destTableName ) {
        String sql ="select distinct customer_id from "+tableName
        		    + " where customer_id is not null and customer_id <> '' ";

        return DataFrameUtil.getDataFrame(sqlContext, sql, destTableName);
    }
    
    
    private DataFrame getLifePolicyFromCustomerId(HiveContext sqlContext,String tableName,String destTableName ) {
    	
    	String sql="select a.customer_id,a.lrt_id,a.lia_policyno,a.lia_accepttime,a.premium,a.from_id from "
    			+ "(select uvp.policyholder_id as customer_id,uvp.lrt_id, uvp.lia_policyno,"
    			+ "uvp.lia_accepttime,uvp.lia_premium as premium ,uvp.handle_clientid as from_id "
    			+ " from "+tableName +" dc inner join  f_net_policysummary uvp on dc.customer_id=uvp.policyholder_id ) a"
    			+ " where a.customer_id is not null  and a.lrt_id is not null"
    			+ " and a.premium <> 0 and a.premium <> '0'"
    			+ " group by a.customer_id,a.lrt_id,a.lia_policyno,a.lia_accepttime,a.premium,a.from_id" ;
    	
        return DataFrameUtil.getDataFrame(sqlContext, sql, destTableName);
    }

    private DataFrame getLifePolicyinstranceType(HiveContext sqlContext,String tableName1,String destName){
        
    	String sql="select a.customer_id,a.lrt_id,a.lia_policyno,a.lia_accepttime,"
        		+ " it.type as insuranceType,a.premium,a.from_id " 
                + " from "+tableName1+" a left join tknwdb.instrance_type it on it.lrtid=a.lrt_id";
                
        return DataFrameUtil.getDataFrame(sqlContext, sql, destName);
    }

  
    /**
     * 找出在 从推荐页面承保的，已经使用过的保单号。后面找非推荐时要排除这些保单号
     * @param sqlContext
     * @return
     */
    private DataFrame getUsedPolicyNo(HiveContext sqlContext,String tableName,String srcTable){
        String sql="select  lia_policyno  from "+tableName+" group by lia_policyno";
        return DataFrameUtil.getDataFrame(sqlContext, sql,srcTable,DataFrameUtil.CACHETABLE_PARQUET);
    }

    /**
     * 从用户拥有的保单记录里排除已经使用过的
     * @return
     */
   
    		
    		 
    private DataFrame getLifeNonUsedPolicyNo(HiveContext sqlContext,String tableName1,String tableName2,String destName){
        String sql=" select t.customer_id,  " +
                "t.lrt_id,  " +
                "t.lia_policyno,  " +
                "t.lia_accepttime, " +
                "t.premium,t.from_id " +
                " from (select up.customer_id,  " +
                "up.lrt_id,  " +
                "up.lia_policyno,  " +
                "up.lia_accepttime, " +
                "up.premium,"+
                "up.from_id  " +
                "from "+tableName1+" up left join "+tableName2+" usedp on up.lia_policyno=usedp.lia_policyno " +
                " where (usedp.lia_policyno is null or usedp.lia_policyno='')  ) t ";
        return DataFrameUtil.getDataFrame(sqlContext, sql, destName);
    }
    
    private DataFrame getNonPropertyUsedPolicyNo(HiveContext sqlContext,String tableName1,String tableName2,String destName){
        String sql=" select t.insuranceType,  " +
                "t.lrt_id,  " +
                "t.lia_policyno,  " +
                "t.lia_accepttime, " +
                "t.premium,t.from_id,t.identifynumber,t.name " +
                " from (select up.insuranceType,  " +
                "up.lrt_id,  " +
                "up.lia_policyno,  " +
                "up.lia_accepttime, " +
                "up.premium,"+
                "up.from_id,up.identifynumber,up.name  " +
                " from "+tableName1+" up left join "+tableName2+" usedp on up.lia_policyno=usedp.lia_policyno " +
                " where (usedp.lia_policyno is null or usedp.lia_policyno='')  ) t ";
        return DataFrameUtil.getDataFrame(sqlContext, sql, destName);
    }




   
    
    
    
    private DataFrame getEarliestAsResult(HiveContext sqlContext,String tableName,String destName,String is_recomend){
        String sql="select user_id,member_id,open_id,customer_id,lrt_id,product_name,info_type,lia_policyno,lia_accepttime,insuranceType, " +
                "max(cast(time as bigint)) as visit_time,'"+is_recomend+"' as is_recomend,premium,from_id " +
                " from "+tableName+" group by " +
                "user_id,member_id,open_id,customer_id,lrt_id,product_name,lia_policyno,lia_accepttime,premium,info_type,from_id,insuranceType";
        return DataFrameUtil.getDataFrame(sqlContext, sql, destName,DataFrameUtil.CACHETABLE_PARQUET);
    }
 

    
    
    
    private DataFrame getPolicyAndTimeAsResult(HiveContext sqlContext,String tableName,String destName){
        String sql="select lia_policyno,max(visit_time) as visit_time from "+tableName+" group by lia_policyno";
        return DataFrameUtil.getDataFrame(sqlContext, sql, destName,DataFrameUtil.CACHETABLE_PARQUET);
    }
    
   
    
    private DataFrame getDistictPolicyAsResult(HiveContext sqlContext,String dirctionTable,String tableName ,String destName){
        String sql="select b.user_id,b.member_id,b.open_id,b.customer_id,b.lrt_id,b.product_name,b.info_type,a.lia_policyno,b.lia_accepttime,a.visit_time,b.is_recomend,b.premium,b.from_id,b.insuranceType" +
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
   

    private DataFrame getRecomendAndNonRecomendUpByFromId(HiveContext sqlContext) {
        String sql="select other.user_id,other.member_id,other.open_id,other.customer_id,other.lrt_id,other.product_name,"
        		+ " case when plat.platform='微信' then '0' "
        		+ "      when plat.platform='WAP' then '1' "
//        		+ "      when plat.platform='APP' and other.info_type ='3' then '3'"
//        		+ "      when plat.platform='APP' and other.info_type ='5' then '5'"
//        		+ "      when plat.platform='APP' and other.info_type ='6' then '6'"
        		+ "      when plat.platform='APP'  then '3' " 
//        		+ "      when plat.platform='PC' and other.info_type ='4' then '4'  else '2' "
        		+ "      when plat.platform='PC' then '2'end as info_type, "
        		+ "     other.lia_policyno,other.lia_accepttime,other.visit_time,other.is_recomend,other.insuranceType,other.premium,other.from_id " +
                " from tknwdb.handle_client plat inner join  MergeRecomendAndNonRecomendUP_TEMP other on other.from_id = plat.from_id where plat.platform='微信' or plat.platform='WAP' or plat.platform='PC' or plat.platform='APP'";
        return DataFrameUtil.getDataFrame(sqlContext, sql, "MergeRecomendAndNonRecomendUP");
    } 

    private DataFrame getNonOwnerOtherPolicyData(HiveContext sqlContext) {
        String sql="select other.user_id,other.member_id,other.open_id,other.customer_id,other.lrt_id,other.product_name,other.info_type,other.lia_policyno,cast(from_unixtime(cast(unix_timestamp(other.lia_accepttime) as bigint), 'yyyy-MM-dd HH:mm:ss') as  string) lia_accepttime,cast(from_unixtime(cast(other.visit_time/1000 as bigint), 'yyyy-MM-dd HH:mm:ss') as  string) visit_time,other.is_recomend,other.insuranceType,other.premium,other.from_id,find.name " +
                " from  tknwdb.handle_client find  inner join MergeRecomendAndNonRecomendUP_TEMP other  on other.from_id = find.from_id where find.platform <> '微信' and find.platform <> 'WAP' and find.platform <> 'PC' and find.platform <> 'APP' ";
        return DataFrameUtil.getDataFrame(sqlContext, sql, "NonOwnerOtherPolicyData");
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


    private DataFrame getSummeryUserPolicyConnectBrowserLife(HiveContext sqlContext, String tableName1, String tableName, String destName) {
        String sql="select rv.user_id,rv.member_id,rv.open_id,rv.lrt_id,rv.product_name,rv.time,rv.info_type," +
                "up.lia_policyno,up.lia_accepttime,up.premium,up.customer_id,up.from_id,up.insuranceType " +
                " from "+tableName1+" up  inner join "+tableName+" rv on up.customer_id=rv.customer_id and " +
                "  up.lrt_id=rv.lrt_id where rv.customer_id is not null and rv.customer_id <> '' and  unix_timestamp(up.lia_accepttime) >= " +
                "cast(rv.time/1000 as bigint)  ";
        return DataFrameUtil.getDataFrame(sqlContext, sql, destName);
    }
    
    private DataFrame getRecommenVisitNoCidNumber(HiveContext sqlContext,String tableName,String destName) {
        String sql="select terminal_id, "
        		+"       user_id, "
        		+"       member_id, "
        		+"   customer_id,cid_number,name,  "
        		+"       open_id, "
        		+"       subtype, "
        		+"       time, "
        		+"       event, "
        		+"       lrt_id, "
        		+"       product_name, "
        		+"       info_type, "
        		+"       is_recomend "
        		+"   from "+tableName 
        		+ "   where cid_number is not null and cid_number <> '' ";
        return DataFrameUtil.getDataFrame(sqlContext, sql, destName);
    }
    
    
    private DataFrame getSummeryUserPolicyConnectBrowserProperty(HiveContext sqlContext, String tableName1, String tableName, String destName) {
        String sql="select rv.user_id,rv.member_id,rv.open_id,rv.lrt_id,rv.product_name,rv.time,rv.info_type," +
                "up.lia_policyno,up.lia_accepttime,up.premium,rv.customer_id,up.from_id,up.insuranceType " +
                " from "+tableName1+" up  inner join "+tableName+" rv on up.identifynumber=rv.cid_number and  up.name=rv.name and " +
                "  up.lrt_id=rv.lrt_id where unix_timestamp(up.lia_accepttime) >= " +
                "cast(rv.time/1000 as bigint) ";
        return DataFrameUtil.getDataFrame(sqlContext, sql, destName);
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
                "lrt_id,subtype,time,event,product_name,is_recomend,info_type from RecommendCommonUserPageVisit "
        		/*+ " union all "+
                "select terminal_id,user_id,member_id,open_id," +
                "lrt_id,subtype,time,event,product_name,is_recomend,info_type from NonRecommendCommonUserPageVisit "*/;
        return DataFrameUtil.getDataFrame(sqlContext, sql, "MergeRecomendAndNonRecomendPB", DataFrameUtil.CACHETABLE_EAGER);

    }
    private DataFrame MergeRecomendAndNonRecomendUP(HiveContext sqlContext){
        String sql=" select user_id,member_id,open_id,customer_id,lrt_id,product_name,lia_policyno,lia_accepttime,visit_time,is_recomend,info_type,insuranceType,premium,from_id " +
                   " from EarliestPolicyResult_life " +
                   " union all " +
                   " select user_id,member_id,open_id,customer_id,lrt_id,product_name,lia_policyno,lia_accepttime,visit_time,is_recomend,info_type,insuranceType,premium,from_id " +
                   " from EarliestPolicyResult_property "/* +
                   " union all " +
                   " select user_id,member_id,open_id,customer_id,lrt_id,product_name,lia_policyno,lia_accepttime,visit_time,is_recomend,info_type,insuranceType,premium,from_id " +
                   " from NonEarliestPolicyResult_life " +
                   " union all " +
                   " select user_id,member_id,open_id,customer_id,lrt_id,product_name,lia_policyno,lia_accepttime,visit_time,is_recomend,info_type,insuranceType,premium,from_id  " +
                   " from NonEarliestPolicyResult_property "*/;
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
    private DataFrame getPropertyInsurancePolicy(HiveContext sqlContext,String tableName,String destName){
        String sql="select distinct p.lia_policyno,p.premium,p.lia_accepttime, p.insuranceType,"
        		+ " c.splancode as lrt_id,p.from_id,p.identifynumber,p.name from " + tableName 
                + "  p inner join tkubdb.gschannelplancode c on c.dplancode=p.flowid where c.dplancode is not null and " 
                + " p.flowid is not null ";
        return DataFrameUtil.getDataFrame(sqlContext, sql,destName);
    }
    
    private DataFrame getPropertyInsurancePolicyTemp(HiveContext sqlContext,String tableName1,String tableName2,String destName) {
        String sql = " select g.policyno,g.identifynumber,g.insuredname as name from "+tableName1+"  a   "
        		+ "     inner join "+tableName2+" g on a.cid_number=g.identifynumber and a.name=g.insuredname " ;
        return DataFrameUtil.getDataFrame(sqlContext, sql, destName);
    }
    /**
     * 关联财险用户表和寿险投保人表，得到用户信息。(带有财险保单）
     * @param sqlContext
     * @return
     */
    private DataFrame getSummaryInsurancePolicy(HiveContext sqlContext,String tableName1,String tableName2,String destName) {
        String sql = " select p.policyno as lia_policyno,p.sumgrosspremium as premium,p.operatedate as lia_accepttime,'财险' as insuranceType,"
        		+ "   p.surveyind as from_id,g.identifynumber,g.name,p.flowid "
        		+ "     from "+tableName1+" g  inner join "+tableName2+"  p " 
                + "      on p.policyno=g.policyno ";
        return DataFrameUtil.getDataFrame(sqlContext, sql,destName );
    }

   
    
    private DataFrame getPropertyDistinctIdentifynumber(HiveContext sqlContext,String tableName,String destTable) {
    	
    	  String sql ="select distinct cid_number,name from  " + tableName
      		    + " where cid_number is not null and cid_number <> '' ";
        
    	return DataFrameUtil.getDataFrame(sqlContext, sql,destTable);
    }
    
 private DataFrame getPropertyPolicyFromCidNumber(HiveContext sqlContext,String tableName,String destTableName ) {
    	
    	String sql="select a.lia_policyno,a.premium,a.lia_accepttime,a.insuranceType,"
    			+ " a.lrt_id,a.from_id,a.identifynumber,a.name from "
    			+ " (select uvp.lia_policyno,uvp.premium, uvp.lia_accepttime,"
    			+ " uvp.insuranceType,uvp.lrt_id,uvp.from_id,uvp.identifynumber,uvp.name "
    			+ " from "+tableName+" dc inner join  InsurancePolicy uvp "
    			+ " on dc.cid_number=uvp.identifynumber and dc.name=uvp.name ) a"
    			+ " where  a.lrt_id is not null"
    			+ " and a.premium <> 0 and a.premium <> '0' "
    			+ " group by a.lia_policyno,a.premium,a.lia_accepttime,a.insuranceType,a.lrt_id,a.from_id,a.identifynumber,a.name " ;
    	
        return DataFrameUtil.getDataFrame(sqlContext, sql, destTableName);
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
    
    private DataFrame getUserBrowserPolicyDetailUnique(HiveContext sqlContext,String srcTable,String desttableName){

     String sql=" select a.user_id_list [0] as user_id, "+
       "a.member_id_list [0] as member_id, "+
       "a.open_id_list [0] as open_id, "+
       "a.customer_id_list [0] as customer_id, "+
       "lrt_id,product_name,lia_policyno,info_type, "+
       "lia_accepttime,visit_time,is_recomend, "+
       "cast(cast(premium as float)/100 as string) as premium, "+
       "short_insurance,long_insurance,preser_insurance, "+
       "finance_insurance,undirect_flag,direct_flag "+
  "from (select collect_list(user_id) user_id_list, "+
               "collect_list(member_id) member_id_list, "+
               "collect_list(open_id) open_id_list, "+
               "collect_list(customer_id) customer_id_list, "+
               "lrt_id, product_name,lia_policyno,info_type, "+
               "lia_accepttime,visit_time,is_recomend,premium, "+
			   "short_insurance,long_insurance,preser_insurance, "+
			   "finance_insurance,undirect_flag,direct_flag "+
          "from (select user_id, member_id, open_id,customer_id, "+
                       "lrt_id,product_name,lia_policyno,info_type, "+
                       "lia_accepttime,visit_time,is_recomend, "+
                       "cast(premium as float)*100 as premium, "+
                       "short_insurance,long_insurance,preser_insurance, "+
                       "finance_insurance,undirect_flag,direct_flag "+
                  "from "+srcTable+ 
                 " order by user_id desc) t "+
         "group by lrt_id, product_name,lia_policyno,info_type, "+
                  "lia_accepttime,visit_time,is_recomend, premium,short_insurance, "+
				  "long_insurance,preser_insurance,finance_insurance, "+
                  "undirect_flag,direct_flag) a ";
        return DataFrameUtil.getDataFrame(sqlContext, sql, desttableName);
    }
    
    /**
     * 推荐监控的  详细情况
     * info_type=0 微信
     * info_type=1 wap
     * info_type=2 pc(包括首页推荐和shop首页推荐), 4(shop首页推荐)
     * info_type=3 app(包括home,search ,searchNoResult) ,5(search),6(searchNoResult)
     */
    public DataFrame getRecomendWithoutAlgoEvalDetail(HiveContext sqlContext){
        //将大健康表空id转化,防止倾斜
    	DataFrame dfdjk=getFactHealthcreditscoreresultNotNullDF(sqlContext);
//        saveAsParquet(dfdjk,"/user/tkonline/taikangtrack_test/data/getFactHealthcreditscoreresultNotNullDF");
//    	System.out.println("大健康表&&&&&&&&&&&&&&&&&&&&"+ dfdjk.take(30).length);
    	
    	//-------微信端推荐页面浏览数据 info_type=0
        DataFrame dfwx=getRecommendWxPageVisit(sqlContext);
//        saveAsParquet(dfwx,"/user/tkonline/taikangtrack_test/data/recomend_wx");
//        System.out.println("微信浏览记录&&&&&&&&&&&&&&&&&&&&"+ dfwx.take(30).length);
        
        //微信链接大健康表
        DataFrame dfwx1= getOpenIdAndUserID(sqlContext,"RecommendWxPageVisit","RecommendUserWxPageVisit","是");
//        saveAsParquet(dfwx1,"/user/tkonline/taikangtrack_test/data/recomend_wx1da");
//        System.out.println("微信关联大健康表&&&&&&&&&&&&&&&&&&&&"+ dfwx1.take(30).length);
        
        //-------wap端推荐页面浏览数据 info_type=1
        DataFrame dfwap=getRecommenWapPageVisit(sqlContext);
//        saveAsParquet(dfwap,"/user/tkonline/taikangtrack_test/data/recomend_wap");
        //-------pc端推荐页面浏览数据 info_type=2 
        DataFrame dfpc=getRecommendPcPageVisit(sqlContext);
//        saveAsParquet(dfpc,"/user/tkonline/taikangtrack_test/data/recomend_pc");

      //-------app端推荐页面浏览数据 info_type=3  
        DataFrame dfApp=getRecommendAppPageVisit(sqlContext);
//      saveAsParquet(dfApp,"/user/tkonline/taikangtrack_test/data/recomend_app"); 
        
        
       // WAP和pc端和APP端数据合并
        DataFrame dftotal= getMergePcAndWapPageVisit(sqlContext,"RecommendWapPageVisit","RecommendPcPageVisit","RecommendAppPageVisit","RecommendWapAndPcPageVisit");
//        System.out.println("合并三端浏览记录&&&&&&&&&&&&&&&&&&&&"+ dftotal.take(30).length);
        //用WAP 和pc端和APP端数据合并的数据关联大健康表
        DataFrame dftotal2=getPcIdAndWapIdAndUserID(sqlContext,"RecommendWapAndPcPageVisit","RecommendUserWapAndPcPageVisit","是");
//        System.out.println("合并三端浏览记录关联大健康表&&&&&&&&&&&&&&&&&&&&"+ dftotal2.take(30).length);
        
        //合并四端的数据
        DataFrame dfRemmendAll=getRecommendAllUserID(sqlContext,"RecommendUserWxPageVisit","RecommendUserWapAndPcPageVisit","RecommendCommonUserPageVisit");
//        saveAsParquet(dfRemmendAll,"/user/tkonline/taikangtrack_test/data/recomend_all");
//        System.out.println("四端数据&&&&&&&&&&&&&&&&&&&&"+ dfRemmendAll.take(30).length);
//        App.loadParquet(sqlContext, "/user/tkonline/taikangtrack_test/data/recomend_all", "RecommendCommonUserPageVisit", false);
      
        //浏览记录中customer_id不为空的
        getRecommenVisitNoCustomerId(sqlContext,"RecommendCommonUserPageVisit","RecommenVisitNoCustomerId");
        
        //吧浏览记录中的所有customer_id 那出来
        DataFrame dfLifeDistinctCustomerId=getLifeDistinctCustomerId(sqlContext,"RecommenVisitNoCustomerId","LifeDistinctCustomerId");
//        System.out.println("浏览记录的customer_id&&&&&&&&&&&&&&&&&&&&"+ dfLifeDistinctCustomerId.take(30).length);
        
        //吧寿险保单表中有浏览记录的customer_id取出来
        DataFrame dfLifePolicyFromCustomerId=getLifePolicyFromCustomerId(sqlContext,"LifeDistinctCustomerId","LifePolicyFromCustomerId");
//        System.out.println("寿险保单的customer_id&&&&&&&&&&&&&&&&&&&&"+ dfLifePolicyFromCustomerId.take(30).length);
        
        //寿险险种分类 长 短 理财
        DataFrame LifePolicySummary=getLifePolicyinstranceType(sqlContext,"LifePolicyFromCustomerId","LifePolicySummary");
//        saveAsParquet(LifePolicySummary,"/user/tkonline/taikangtrack_test/data/LifePolicySummary");
//        System.out.println("寿险保单的长短理财&&&&&&&&&&&&&&&&&&&&"+ LifePolicySummary.take(30).length);
                      
        //总浏览记录关联寿险总保单
        DataFrame summeryDF = getSummeryUserPolicyConnectBrowserLife(sqlContext, "LifePolicySummary", "RecommenVisitNoCustomerId", "UserPolicyConnectBrowser_all_temp_life");
//        saveAsParquet(summeryDF,"/user/tkonline/taikangtrack_test/data/UserPolicyConnectBrowser_all_temp");
//        System.out.println("浏览记录关联寿险&&&&&&&&&&&&&&&&&&&&"+ summeryDF.take(30).length);
        
        DataFrame summeryDF1 = getEarliestAsResult(sqlContext, "UserPolicyConnectBrowser_all_temp_life", "EarliestPolicyResult_early_temp_life", "是");
//        saveAsParquet(summeryDF1,"/user/tkonline/taikangtrack_test/data/EarliestPolicyResult_early_temp");
//        System.out.println("浏览记录关联寿险1&&&&&&&&&&&&&&&&&&&&"+ summeryDF1.take(30).length);
        
        DataFrame summeryDF2 = getPolicyAndTimeAsResult(sqlContext,"EarliestPolicyResult_early_temp_life","EarliestPolicyResult_dirction_temp_life");
//        saveAsParquet(summeryDF2,"/user/tkonline/taikangtrack_test/data/EarliestPolicyResult_dirction_temp");
//        System.out.println("浏览记录关联寿险2&&&&&&&&&&&&&&&&&&&&"+ summeryDF2.take(30).length);
        
        DataFrame summeryDF277 =getDistictPolicyAsResult(sqlContext,"EarliestPolicyResult_dirction_temp_life","EarliestPolicyResult_early_temp_life","EarliestPolicyResult_life");
//        saveAsParquet(summeryDF277,"/user/tkonline/taikangtrack_test/data/EarliestPolicyResult");
//        System.out.println("浏览记录关联寿险3&&&&&&&&&&&&&&&&&&&&"+ summeryDF277.take(30).length);
        
    	//取财险身份证号不为空的记录
        getRecommenVisitNoCidNumber(sqlContext,"RecommendCommonUserPageVisit","RecommenVisitNoCidNumber");
        
      //吧浏览记录中的所有身份证号 那出来
        DataFrame dfPropertyDistinctIdentifynumber=getPropertyDistinctIdentifynumber(sqlContext,"RecommenVisitNoCidNumber","LifeDistinctCidNumber");
//        saveAsParquet(dfPropertyDistinctIdentifynumber, "/user/tkonline/taikangtrack_test/data/dfPropertyDistinctIdentifynumber");
//        System.out.println("财险customer+_id&&&&&&&&&&&&&&&&&&&&"+ dfPropertyDistinctIdentifynumber.take(30).length);
      
        //身份证对应的保单
        DataFrame getPropertyInsurancePolicyTemp=getPropertyInsurancePolicyTemp(sqlContext,"LifeDistinctCidNumber","GUPOLICYRELATEDPARTY","PropertyInsurancePolicyTemp");
//        saveAsParquet(getPropertyInsurancePolicyTemp,"/user/tkonline/taikangtrack_test/data/getPropertyInsurancePolicyTemp");
              
        //人和保单
        DataFrame df2= getSummaryInsurancePolicy(sqlContext,"PropertyInsurancePolicyTemp","GUPOLICYCOPYMAIN","InsurancePolicy");
//        saveAsParquet(df2, "/user/tkonline/taikangtrack_test/data/recomden_evalute_CommonUser");
//        System.out.println("财险用户保单&&&&&&&&&&&&&&&&&&&&"+ df2.take(30).length);
        
        //关联表找出lrt_id
        DataFrame df1= getPropertyInsurancePolicy(sqlContext,"InsurancePolicy", "PropertyInsurancePolicy");
//        saveAsParquet(df1, "/user/tkonline/taikangtrack_test/data/recomden_evalute_propertyinsurancepolicy");
//        System.out.println("计算出财险用户保单信息&&&&&&&&&&&&&&&&&&&&"+ df1.take(30).length);
          
        //全部浏览记录关联全部保单和去重
        DataFrame summeryDF4 = getSummeryUserPolicyConnectBrowserProperty(sqlContext, "PropertyInsurancePolicy", "RecommenVisitNoCidNumber", "UserPolicyConnectBrowser_all_temp_property");
//        System.out.println("浏览记录关联财险&&&&&&&&&&&&&&&&&&&&"+ summeryDF4.take(30).length);
//        saveAsParquet(summeryDF4,"/user/tkonline/taikangtrack_test/data/UserPolicyConnectBrowser_all_temp");
        DataFrame summeryDF14 = getEarliestAsResult(sqlContext, "UserPolicyConnectBrowser_all_temp_property", "EarliestPolicyResult_early_temp_property", "是");
//        saveAsParquet(summeryDF14,"/user/tkonline/taikangtrack_test/data/EarliestPolicyResult_early_temp");
        DataFrame summeryDF24 = getPolicyAndTimeAsResult(sqlContext,"EarliestPolicyResult_early_temp_property","EarliestPolicyResult_dirction_temp_property");
//        saveAsParquet(summeryDF24,"/user/tkonline/taikangtrack_test/data/EarliestPolicyResult_dirction_temp");
        DataFrame summeryDF2774 =getDistictPolicyAsResult(sqlContext,"EarliestPolicyResult_dirction_temp_property","EarliestPolicyResult_early_temp_property","EarliestPolicyResult_property");
//        saveAsParquet(summeryDF2774,"/user/tkonline/taikangtrack_test/data/EarliestPolicyResult");
        
        /*//------非推荐页面微信浏览数据--------------
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
        
        //非推荐页面app浏览数据 
        DataFrame dfNoapp=getNonRecommendAppPageVisit(sqlContext);
//       saveAsParquet(dfNopc,"/user/tkonline/taikangtrack_test/data/Non_recomend_app");
        
        //合并非推荐页面wap和pc和APP浏览数据--------------
        getMergePcAndWapPageVisit(sqlContext,"NonRecommendWapPageVisit","NonRecommendPcPageVisit","NonRecommendAppPageVisit","NonRecommendWapAndPcPageVisit");
        //用合并非推荐页面wap和pc和APP浏览数据和大健康表相连--------------
        getPcIdAndWapIdAndUserID(sqlContext,"NonRecommendWapAndPcPageVisit","NonRecommendUserWapAndPcPageVisit","否");
        //合并非推荐四端端的数据
        DataFrame dfNonRemmendAll=getRecommendAllUserID(sqlContext,"NonRecommendWxUserPageVisit","NonRecommendUserWapAndPcPageVisit","NonRecommendCommonUserPageVisit");
//       saveAsParquet(dfNonRemmendAll,"/user/tkonline/taikangtrack_test/data/non_recomend_all");
       
        //浏览记录中customer_id不为空的
       getRecommenVisitNoCustomerId(sqlContext,"NonRecommendCommonUserPageVisit","NonRecommenVisitNoCustomerId");
       
       //吧浏览记录中的所有customer_id 那出来
       DataFrame dfLifeDistinctCustomerIdNo=getLifeDistinctCustomerId(sqlContext,"NonRecommenVisitNoCustomerId","NoLifeDistinctCustomerId");
       //吧寿险保单表中有浏览记录的customer_id取出来
       DataFrame dfLifePolicyFromCustomerIdNo=getLifePolicyFromCustomerId(sqlContext,"NoLifeDistinctCustomerId","NoLifePolicyFromCustomerId");        
       
       //找到寿险推荐保单
        getUsedPolicyNo(sqlContext,"EarliestPolicyResult_life","lifeUsedPolicyNo");
        //寿险总表单减去寿险推荐保单，剩下的就是寿险非推荐的保单
        getLifeNonUsedPolicyNo(sqlContext,"NoLifePolicyFromCustomerId","lifeUsedPolicyNo","NonUsedLifeInstancetTypeNo");
        
      //寿险险种分类 长 短 理财
        DataFrame LifePolicySummaryNo=getLifePolicyinstranceType(sqlContext,"NonUsedLifeInstancetTypeNo","NonUsedLifePolicyNo");
        
        //没加from_id时，非推荐的保单和去重
        DataFrame summeryDF23 = getSummeryUserPolicyConnectBrowserLife(sqlContext, "NonUsedLifePolicyNo", "NonRecommenVisitNoCustomerId", "NonUserPolicyConnectBrowser_all_temp_life");
//        saveAsParquet(summeryDF23,"/user/tkonline/taikangtrack_test/data/NonUserPolicyConnectBrowser_all_temp");
        DataFrame summeryDF3 = getEarliestAsResult(sqlContext, "NonUserPolicyConnectBrowser_all_temp_life", "NonEarliestPolicyResult_early_temp_life", "否");
//        saveAsParquet(summeryDF3,"/user/tkonline/taikangtrack_test/data/NonEarliestPolicyResult_early_temp");
        DataFrame summeryDF33=getPolicyAndTimeAsResult(sqlContext,"NonEarliestPolicyResult_early_temp_life","NonEarliestPolicyResult_dirction_temp_life");
//        saveAsParquet(summeryDF33,"/user/tkonline/taikangtrack_test/data/NonEarliestPolicyResult_dirction_temp");
        DataFrame summeryDF335= getDistictPolicyAsResult(sqlContext,"NonEarliestPolicyResult_dirction_temp_life","NonEarliestPolicyResult_early_temp_life","NonEarliestPolicyResult_life");
//        saveAsParquet(summeryDF335,"/user/tkonline/taikangtrack_test/data/NonEarliestPolicyResult");
       
      
      //取财险身份证号不为空的记录
        getRecommenVisitNoCidNumber(sqlContext,"NonRecommendCommonUserPageVisit","NonRecommenVisitNoCidNumber");
        
      //吧浏览记录中的所有身份证号 那出来
        DataFrame NondfPropertyDistinctIdentifynumber=getPropertyDistinctIdentifynumber(sqlContext,"NonRecommenVisitNoCidNumber","NonLifeDistinctCidNumber");
//        saveAsParquet(dfPropertyDistinctIdentifynumber, "/user/tkonline/taikangtrack_test/data/NondfPropertyDistinctIdentifynumber");
//        System.out.println("财险customer+_id&&&&&&&&&&&&&&&&&&&&"+ dfPropertyDistinctIdentifynumber.take(30).length);
      
        //身份证对应的保单
        DataFrame NongetPropertyInsurancePolicyTemp=getPropertyInsurancePolicyTemp(sqlContext,"NonLifeDistinctCidNumber","GUPOLICYRELATEDPARTY","NonPropertyInsurancePolicyTemp");
//        saveAsParquet(getPropertyInsurancePolicyTemp,"/user/tkonline/taikangtrack_test/data/NongetPropertyInsurancePolicyTemp");
              
        //人和保单
        DataFrame Nondf2= getSummaryInsurancePolicy(sqlContext,"NonPropertyInsurancePolicyTemp","GUPOLICYCOPYMAIN","NonInsurancePolicy");
//        saveAsParquet(df2, "/user/tkonline/taikangtrack_test/data/Nonrecomden_evalute_CommonUser");
//        System.out.println("财险用户保单&&&&&&&&&&&&&&&&&&&&"+ df2.take(30).length);
        
        //关联表找出lrt_id
        DataFrame Nondf1= getPropertyInsurancePolicy(sqlContext,"NonInsurancePolicy", "NonPropertyInsurancePolicy");
//        saveAsParquet(df1, "/user/tkonline/taikangtrack_test/data/Nonrecomden_evalute_propertyinsurancepolicy");
//        System.out.println("计算出财险用户保单信息&&&&&&&&&&&&&&&&&&&&"+ df1.take(30).length);
          
        //找到财险推荐保单
        getUsedPolicyNo(sqlContext,"EarliestPolicyResult_property","propertyUsedPolicyNo");
        //财险总表单减去寿险推荐保单，剩下的就是财险非推荐的保单
        getNonPropertyUsedPolicyNo(sqlContext,"NonPropertyInsurancePolicy","propertyUsedPolicyNo","NonUsedPropertyPolicyNo");
        //全部浏览记录关联全部保单和去重
        DataFrame summeryDF45 = getSummeryUserPolicyConnectBrowserProperty(sqlContext, "NonUsedPropertyPolicyNo", "NonRecommenVisitNoCidNumber", "NonUserPolicyConnectBrowser_all_temp_property");
//        saveAsParquet(summeryDF45,"/user/tkonline/taikangtrack_test/data/UserPolicyConnectBrowser_all_temp");
        DataFrame summeryDF145 = getEarliestAsResult(sqlContext, "NonUserPolicyConnectBrowser_all_temp_property", "NonEarliestPolicyResult_early_temp_property", "否");
//        saveAsParquet(summeryDF145,"/user/tkonline/taikangtrack_test/data/EarliestPolicyResult_early_temp");
        DataFrame summeryDF245 = getPolicyAndTimeAsResult(sqlContext,"NonEarliestPolicyResult_early_temp_property","NonEarliestPolicyResult_dirction_temp_property");
//                saveAsParquet(summeryDF24,"/user/tkonline/taikangtrack_test/data/EarliestPolicyResult_dirction_temp");
        DataFrame summeryDF27745 =getDistictPolicyAsResult(sqlContext,"NonEarliestPolicyResult_dirction_temp_property","NonEarliestPolicyResult_early_temp_property","NonEarliestPolicyResult_property");
//                saveAsParquet(summeryDF2774,"/user/tkonline/taikangtrack_test/data/EarliestPolicyResult");
        */

        //      合并两个页面浏览记录
        DataFrame m1=MergeRecomendAndNonRecomendPB(sqlContext);
//        saveAsParquet(m1,"/user/tkonline/taikangtrack_test/data/recomden_evalute_m1");
//        System.out.println("合并两个页面浏览记录&&&&&&&&&&&&&&&&&&&&"+ m1.take(30).length);
        
        //合并财险和寿险保单记录
        DataFrame m2=MergeRecomendAndNonRecomendUP(sqlContext);
//        saveAsParquet(m2,"/user/tkonline/taikangtrack_test/data/recomden_evalute_m2");
//        System.out.println("合并财险和寿险保单记录&&&&&&&&&&&&&&&&&&&&"+ m2.take(30).length);
        
        String sysDate = GetSysDate(0);
        //用from_id来区分的自有平台
        DataFrame getRecomendAndNonRecomendUpByFromId = getRecomendAndNonRecomendUpByFromId(sqlContext);
//        saveAsParquet(getRecomendAndNonRecomendUpByFromId,"/user/tkonline/taikangtrack_test/data/recomden_evalute_other_feiziyou");
//        System.out.println("用from_id来区分的自有平台&&&&&&&&&&&&&&&&&&&&"+ getRecomendAndNonRecomendUpByFromId.take(30).length);
        //非自有平台的from_id
        DataFrame NonOwner = getNonOwnerOtherPolicyData(sqlContext);
        saveAsParquet(NonOwner,"/user/tkonline/taikangtrack/data/recomend_evaluate_detail_nonowner-"+sysDate);

        //推荐和非推 用户行为对应保单表 合并后，把它转化为详情表
        DataFrame dfDetail=getUserBrowserPolicyDetail(sqlContext, "MergeRecomendAndNonRecomendUP", "UserBrowserPolicyDetail2");
//        saveAsParquet(dfDetail,"/user/tkonline/taikangtrack_test/data/recomden_evalute_dfDetail");
//        System.out.println("详情表&&&&&&&&&&&&&&&&&&&&"+ dfDetail.take(30).length);
        //保单去重
        DataFrame dfDetailUnique=getUserBrowserPolicyDetailUnique(sqlContext,"UserBrowserPolicyDetail2","UserBrowserPolicyDetail");
        
        return dfDetailUnique;
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
