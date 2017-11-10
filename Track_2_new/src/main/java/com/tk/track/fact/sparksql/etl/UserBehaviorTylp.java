package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF10;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF9;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;

import com.tk.track.fact.sparksql.desttable.FactUserBehaviorTele;
import com.tk.track.fact.sparksql.desttable.FactUserBehaviorTylp;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

public class UserBehaviorTylp implements Serializable{

	private static final long serialVersionUID = 1L;
    
	public String getbeforeday(int beforeday){
		Date date=new Date();
		Calendar calendar=Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.DAY_OF_MONTH,beforeday);
		SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd");
		return format.format(calendar.getTime());
	}
	
	
	public void createConcatUdf(HiveContext sqlContext){
		sqlContext.udf().register("concat10", new UDF10<String,String,String,String,String,String,String,String,String,String,String>(){
			     public String call(String arg1,String arg2,String arg3,String arg4,String arg5,String arg6,String arg7,String arg8,String arg9,String arg10){
			    	 if(arg1==null){
			    		 arg1="";
			    	 }
			    	 if(arg2==null){
			    		 arg2="";
			    	 }
			    	 if(arg3==null){
			    		 arg3="";
			    	 }
                     if(arg4==null){
			    		 arg4="";
			    	 }
                     if(arg5==null){
			    		 arg5="";
			    	 }
                     if(arg6==null){
			    		 arg6="";
			    	 }
                     if(arg7==null){
			    		 arg7="";
			    	 }
                     if(arg8==null){
			    		 arg8="";
			    	 }
                     if(arg9==null){
			    		 arg9="";
			    	 }
                     if(arg10==null){
			    		 arg10="";
			    	 }
			    	 return arg1+arg2+arg3+arg4+arg5+arg6+arg7+arg8+arg9+arg10;
			     }
		},DataTypes.StringType );
	}
	
	public void createConcatUdf3(HiveContext sqlContext){
		sqlContext.udf().register("concat3", new UDF3<String,String,String,String>(){
			     public String call(String arg1,String arg2,String arg3){
			    	if(arg1==null){
			    		arg1="";
			    	}
			    	if(arg2==null){
			    		arg2="";
			    	}
			    	if(arg3==null){
			    		arg3="";
			    	}
			    	 return arg1+arg2+arg3;
			     }
		},DataTypes.StringType );
	}
	
	public void createTyDf(HiveContext sqlContext){
		String hql1="select user_id,"
      +"             substring(substring(label,10), 1, length(substring(label,10)) - 1) as clamid"
      +"             from FACT_STATISTICS_EVENT"
      +"             where app_id = 'wechat006'"
      +"             and from_unixtime(int(visit_time / 1000), 'yyyy-MM-dd') ="
      +"                '"+getbeforeday(-1)+"'"
      +"             and label like '%claimId%'";
		
		DataFrameUtil.getDataFrame(sqlContext, hql1, "user_claim");
		
	   String hql3="select  cc.claim_id,type.lrt_name,cc.claim_type,c.last_amount,c.refuse"
  +"                 from tkoldb.clm_case c"
  +"                 inner join tkoldb.clm_inquire i"
  +"                 on i.inquire_id = c.inquire_id"
  +"                 inner join tkoldb.claim_commoninfo cc"
  +"                 on cc.claim_id = i.claim_id"
  //+"                 inner join tkoldb.p_cscpolicyno pcp on c.lia_policyno = pcp.csc_policyno"
 // +"                 inner join tkoldb.t_insurelist ti on pcp.lia_policyno = ti.lia_policyno";
  +"                 inner join P_LIFEINSURE life on c.lia_policyno = life.lia_policyno"
  +"                 inner join tkoldb.d_liferisktype type on life.lrt_id=type.lrt_id";
	 
	
		
		
		
		
		DataFrameUtil.getDataFrame(sqlContext, hql3, "user_information");
		   

	   String hql4="select c.user_id,i.* from user_claim  c inner join user_information i on c.clamid=i.claim_id";
	   DataFrameUtil.getDataFrame(sqlContext, hql4, "user_claim_infomation");
	   
	   
	   String hql5 = "SELECT DISTINCT AA.OPEN_ID, AA.MEMBER_ID,AA.CUSTOMER_ID,CASE"
				+"         WHEN AA.MEMBER_ID IS NOT NULL AND AA.MEMBER_ID <> '' THEN"
				+"          'MEM'"
				+"         WHEN AA.CUSTOMER_ID IS NOT NULL AND AA.CUSTOMER_ID <> '' THEN"
				+"          'C'"
				+"         ELSE"
				+"          'WE'"
				+"       END AS USER_TYPE"
				+ "  FROM (SELECT USER_ID, OPEN_ID, MEMBER_ID,CUSTOMER_ID , NAME, BIRTHDAY, GENDER"
				+ "          FROM FACT_USERINFO TFU"
				+ "         WHERE OPEN_ID IS NOT NULL AND OPEN_ID <> '') AA"
				+ "      ,(SELECT MAX(USER_ID) USER_ID, OPEN_ID"
				+ "          FROM FACT_USERINFO TFU"
				+ "         WHERE OPEN_ID IS NOT NULL AND OPEN_ID <> ''"
				+ "         GROUP BY OPEN_ID) BB"
				+ " WHERE AA.OPEN_ID = BB.OPEN_ID AND AA.USER_ID = BB.USER_ID";
	   
	   DataFrameUtil.getDataFrame(sqlContext, hql5, "USERTYPE_OPENID");
	}
	
	public DataFrame getUserBehaviorTylp(HiveContext sqlContext){
		String hql="select '' rowkey,z.user_id,"
	   +"  us.name,"
	   +"  substr(us.birthday,0,10) birthday,"
	   +"  case when us.gender=0 then '男'  when us.gender=1 then '女'  end as gender,"
	  // +"  case"
	  // +"  when us.name is not null then"
	  // +"  'MEM'"
	  // +"  else"
	  // +"  'WE'"
	  // +"  end as sutype,"
	   +"  z.event,"
	   +"  z.subtype,"
	   +"  '' third_level,"
	   +"  '' fourth_level,"
       +"  z.app_id,"
       +"  z.app_type,"
       +"  string(z.v_num) v_num,"
       +"  string(z.v_duration) v_duration,"
       +"  z.from_id,"
       +"  z.v_time,"
       +"  z.event1,"
       +"  case"
       +"    when z.event1 = 1 then"
       +"     z.event"
       +"    else"
       +"     concat3('我要理赔-引导页首页', '->', z.event)"
       +"  end as path"
       +"  from (select aa.user_id,"
       +"           aa.app_id,"
       +"           aa.app_type,"
       +"           aa.from_id,"
       +"           from_unixtime(int(min(bb.visit_time)/1000),'yyyy-MM-dd HH:mm:ss') v_time,"
       +"          sum(bb.visit_count) v_num,"
       +"          sum(bb.visit_duration) v_duration,"
       +"          bb.event,"
       +"          bb.subtype,"
       +"          aa.event1"
       +"     from (select user_id,"
       +"                 app_id,"
       +"                  from_id,"
       +"                  app_type,"
       +"                 max(case"
       +"                        when event = '我要理赔-引导页首页' then"
       +"                         1"
       +"                        when event = '我要理赔-填写出险信息首页' then"
       +"                         2"
       +"                       when event = '我要理赔-储蓄卡首页' then"
       +"                         3"
       +"                        when event = '我要理赔-支付宝首页' then"
       +"                        4"
       +"                       when event = '我要理赔-身份证首页' then"
       +"                        5"
       +"                       when event = '我要理赔-上传理赔资料首页' then"
       +"                        6"
       +"                       when event = '我要理赔-理赔完成首页' then"
       +"                        7"
       +"                       else"
       +"                        0"
       +"                     end) as event1"
       +"            from FACT_STATISTICS_EVENT"
       +"           where app_id = 'wechat006'"
       +"            and from_unixtime(int(visit_time / 1000), 'yyyy-MM-dd') ="
       +"                '"+getbeforeday(-1)+"'"
       +"          group by user_id, app_id, from_id,app_type"
       +"         having event1 <> 0) aa"
       +"  inner join (select case"
       +"                      when event = '我要理赔-引导页首页' then"
       +"                       1"
       +"                      when event = '我要理赔-填写出险信息首页' then"
       +"                       2"
       +"                      when event = '我要理赔-储蓄卡首页' then"
       +"                       3"
       +"                      when event = '我要理赔-支付宝首页' then"
       +"                       4"
       +"                      when event = '我要理赔-身份证首页' then"
       +"                       5"
       +"                    when event = '我要理赔-上传理赔资料首页' then"
       +"                     6"
       +"                    when event = '我要理赔-理赔完成首页' then"
       +"                      7"
       +"                     else"
       +"                     0"
       +"                   end event1,"
       +"                   user_id,"
       +"                   app_id,"
       +"                    from_id,"
       +"                   visit_time,"
       +"                   visit_count,"
       +"                   visit_duration,"
       +"                   event,"
       +"                   subtype"
       +"              from FACT_STATISTICS_EVENT  where user_id is not null and app_id is not null  and from_id is not null) bb"
       +"     on aa.user_id = bb.user_id"
       +"    and aa.app_id = bb.app_id"
       +"    and aa.event1 = bb.event1"
       +"    and aa.from_id = bb.from_id"
       +"  group by aa.user_id, aa.app_id, aa.from_id, bb.event, aa.event1,bb.subtype,aa.app_type) z"
 +"  left join (select qc.user_id,qc.open_id,u.name,u.birthday,u.gender"
      +"  from FACT_USERINFO u"
      +"  inner join (select max(user_id) user_id, open_id"
      +"    from FACT_USERINFO"
      +"   group by open_id"
      +"  having open_id is not null) qc"
    +"  on u.user_id = qc.user_id"
   +"  and u.open_id = qc.open_id) us"
 +"    on z.user_id = us.open_id ";
		
	 DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_STA_USERINFO_TYLP");
	 
	 String hql2="select t.rowkey AS ROWKEY,o.member_id AS USER_ID,o.user_type AS USER_TYPE,t.app_type AS APP_TYPE,t.app_id AS APP_ID,'flow' as EVENT_TYPE,t.event AS EVENT,t.subtype as SUB_TYPE,t.v_duration as VISIT_DURATION,t.from_id AS FROM_ID,'理赔' as PAGE_TYPE,'武汉营销数据' as  FIRST_LEVEL,'官网会员' as  SECOND_LEVEL,'微信理赔获取' as THIRD_LEVEL,'微信理赔' as  FOURTH_LEVEL,t.v_time as VISIT_TIME,t.v_num as VISIT_COUNT,'2' as CLUE_TYPE,concat10('姓名：',t.name,'，浏览路径：',t.path,'，理赔险种名称：',i.lrt_name,'，理赔金额：',i.last_amount,'，拒赔原因：',i.refuse) as REMARK,t.name as USER_NAME "
	  	       +"         from FACT_STA_USERINFO_TYLP t"
	  	       +"         left join user_claim_infomation i"
	  	       +"         on t.user_id = i.user_id"
	  	       +"         inner join (select * from USERTYPE_OPENID where member_id is not null and member_id <> '') o "
	  	       +"         on t.user_id =o.open_id ";
	 return DataFrameUtil.getDataFrame(sqlContext, hql2, "FACT_STA_USERINFO_TYLP_INFORMATION");
	}
	
	
	public DataFrame getUserBehaviorTylpOther(HiveContext sqlContext){
		String hql="select '' rowkey,"
	  + "aa.user_id,"
	  +" bb.name,"
      +" substr(bb.birthday,0,10) birthday,"
      +" case when bb.gender=0 then '男'  when bb.gender=1 then '女'  end as gender,"
     // +" case"
     // +"    when bb.name is not null then"
    //  +"    'MEM'"
     // +"   else"
     // +"    'WE'"
     // +" end as sutype,"
      +" aa.event,"
      +" aa.subtype,"
      +"  '' third_level,"
	  +"  '' fourth_level,"
      +" aa.app_id,"
      +" aa.app_type,"
      +" string(aa.v_count) v_num,"
      +" string(aa.v_duration) v_duration,"
      +"  aa.from_id,"
      +"  aa.v_time,"
      +"  0 event1," 
      +"  concat3(aa.event, ',', string(aa.v_count)) path"
      +" from (select user_id,"
      +"           event,"
      +"           subtype,"
      +"          sum(visit_count) v_count,"
      +"          sum(visit_duration) v_duration,"
      +"          from_unixtime(int(min(visit_time)/1000),'yyyy-MM-dd HH:mm:ss') v_time,"
      +"         app_id,"
      +"         app_type,"
      +"         from_id"
      +"    from FACT_STATISTICS_EVENT"
      +"    where app_id = 'wechat006'"
      +"      and from_unixtime(int(visit_time / 1000), 'yyyy-MM-dd') ="
      +"                '"+getbeforeday(-1)+"'"
      +"     and event not like '%我要理赔%'"
      +"   group by user_id, event, app_id, from_id,subtype,app_type) aa"
      +"  left join (select qc.user_id,qc.open_id,u.name,u.birthday,u.gender"
      +"  from FACT_USERINFO u"
      +"  inner join (select max(user_id) user_id, open_id"
      +"    from FACT_USERINFO"
      +"   group by open_id"
      +"  having open_id is not null) qc"
    +"  on u.user_id = qc.user_id"
   +"  and u.open_id = qc.open_id) bb"
      +"   on aa.user_id = bb.open_id ";
  	 DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_STA_USERINFO_TYLPOTHER");
  	String hql2="select t.rowkey AS ROWKEY,o.member_id AS USER_ID,o.user_type AS USER_TYPE,t.app_type AS APP_TYPE,t.app_id AS APP_ID,'flow' as EVENT_TYPE,t.event AS EVENT,t.subtype as SUB_TYPE,t.v_duration as VISIT_DURATION,t.from_id AS FROM_ID,'理赔' as PAGE_TYPE,'武汉营销数据' as  FIRST_LEVEL,'官网会员' as  SECOND_LEVEL,'微信理赔获取' as THIRD_LEVEL,'微信理赔' as  FOURTH_LEVEL,t.v_time as VISIT_TIME,t.v_num as VISIT_COUNT,'2' as CLUE_TYPE,concat10('姓名：',t.name,'，浏览路径：',t.path,',理赔险种名称：',i.lrt_name,'，理赔金额：',i.last_amount,'，拒赔原因：',i.refuse) as REMARK,t.name as USER_NAME "
  	       +"         from FACT_STA_USERINFO_TYLPOTHER t"
  	       +"         left join user_claim_infomation i"
  	       +"         on t.user_id = i.user_id"
  	       +"         inner join (select * from USERTYPE_OPENID where member_id is not null and member_id <> '') o "
  	       +"         on t.user_id =o.open_id ";
  	       
  	       
  	       ;
  		 return DataFrameUtil.getDataFrame(sqlContext, hql2, "FACT_STA_USERINFO_TYLPOTHER_INFORMATION");
	}
	
	public JavaRDD<FactUserBehaviorTylp> getJavaRdd(DataFrame df){
		JavaRDD<Row> jRDD = df.select("ROWKEY", "USER_ID","NAME","BIRTHDAY","GENDER","SUTYPE",
				"EVENT","THIRD_LEVEL","FOURTH_LEVEL","APP_ID","V_NUM","FROM_ID","V_TIME","EVENT1","PATH").rdd().toJavaRDD();
		JavaRDD<FactUserBehaviorTylp> rRDD=jRDD.map(new Function<Row,FactUserBehaviorTylp>(){

			public FactUserBehaviorTylp call(Row v1) throws Exception {
				return new FactUserBehaviorTylp(v1.getString(0),
						v1.getString(1),
						v1.getString(2),
						v1.getString(3),
						v1.getString(4),
						v1.getString(5),
						v1.getString(6),
						v1.getString(7),
						v1.getString(8),
						v1.getString(9),
						v1.getString(10),
						v1.getString(11),
						v1.getString(12),
						v1.getString(14)
						);
			}
			
		});
	return rRDD;
	}
	
	
	
	public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<FactUserBehaviorTylp> rdd, String path) {
		if (TK_DataFormatConvertUtil.isExistsPath(path)) {
			sqlContext.createDataFrame(rdd, FactUserBehaviorTylp.class).save(path, "parquet", SaveMode.Append);
		} else {
			sqlContext.createDataFrame(rdd, FactUserBehaviorTylp.class).saveAsParquetFile(path);
		}
	}
	
	
	public void saveasParquet(DataFrame df,String path){
		if (TK_DataFormatConvertUtil.isExistsPath(path)) {
			df.save(path, "parquet", SaveMode.Append);
		} else {
			df.saveAsParquetFile(path);
		}
	}
}
