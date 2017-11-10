package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.main.App;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

public class UserBehaviorBrowseWebsite implements Serializable{
	
	public String getbeforeday(int beforeday){
		Date date=new Date();
		Calendar calendar=Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.DAY_OF_MONTH,beforeday);
		SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd");
		return format.format(calendar.getTime());
	}
	
	 public Boolean loadPolicyInfoResultTable(HiveContext sqlContext) {
	    	boolean succ = false;
	    	String sysdt = App.GetSysDate(-1);
	    	String pathTemp = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTNETPOLICYINFORESULT_OUTPUTPATH);
	    	String path = pathTemp + "-" + sysdt;
	    	if (TK_DataFormatConvertUtil.isExistsPath(path)) {
	    		succ = true;
		    	sqlContext.load(path).registerTempTable("POLICYINFORESULT"); //有效保单
	    	}
	    	return succ;
	    }

	
	
	
	
      public DataFrame getBrowseWebsiteDf(HiveContext sqlContext){
    	  String hql="select e.user_id,"
  +"      e.event,"
  +"      e.subtype,"
  +"      from_unixtime(int(e.visit_time/1000),'yyyy-MM-dd HH:mm:ss') visit_time,"
  +"      e.visit_count,"
  +"           e.visit_duration,"
  +"           e.app_id as infofrom,"
  +"           e.from_id,"
  +"           case when e.label is not null then" 
  +"           substring(substring(e.label,10), 1, length(substring(e.label,10)) - 1)"  
  +"            else "
  +"           e.label"
  +"           end as label"
  +"      from FACT_STATISTICS_EVENT e"
  +"      where from_unixtime(int(e.visit_time/1000),'yyyy-MM-dd') ="
  +"                '"+getbeforeday(-1)+"'"
  +"            and (e.app_id='webSite003' "
  +"            or e.app_id='webSite005' "
  +"           or e.app_id='webSite006')";
    	  
    	  DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_STATISTICS_EVENT_SUB");
    	  
    	  
    	String hql2="select member_id, min(customer_id) customer_id, name"
                    +"  from FACT_USERINFO"
                    +"  group by member_id, name";
    	
    	DataFrameUtil.getDataFrame(sqlContext, hql2, "FACT_USERINFO_SUB");
    	
    	String hql3="select s.*, case"
  +"                 when info.member_id is not null and info.member_id <> '' then" 
  +"                  'MEM'"
  +"                when info.customer_id is not null and info.customer_id <> '' then "
  +"                  'C'"
  +"                 else "
  +"                   ''"
  +"                 end as user_type,"
  +"                 info.member_id,"
  +"                 info.customer_id,"
  +"                 info.name"
  +"                from FACT_STATISTICS_EVENT_SUB s"
  +"                left join FACT_USERINFO_SUB info"
  +"                  on s.user_id = info.member_id";
    	 DataFrameUtil.getDataFrame(sqlContext, hql3, "USER_INFOMATION");
    	 
    	String hql4="select '' rowkey, i.*,case"
  +"          when p.lrt_id is not null and p.lrt_id <> '' "
  +"            then    " 
  +"             '0'  "
  +"            else    " 
  +"            '1'  "
  +"            end as if_pay,"
  +"            p.lrt_id,"
  +"            '' third_level,"
  +"            '' fourth_level,"
  +"            '' calssify_name,"
  +"            '' productname,"
  +"            '' clue_type,"
  +"            '' class_ids"
  +"           from USER_INFOMATION i"
  +"          left join POLICYINFORESULT p"
  +"           on i.customer_id = p.POLICYHOLDER_ID";
    	  return DataFrameUtil.getDataFrame(sqlContext, hql4, "BROWSEWEBSITE_RESULT");
      }
      
      
      
      
      
      public void saveasParquet(DataFrame df,String path){
  		if (TK_DataFormatConvertUtil.isExistsPath(path)) {
  			df.save(path, "parquet", SaveMode.Append);
  		} else {
  			df.saveAsParquetFile(path);
  		}
  	}
}
