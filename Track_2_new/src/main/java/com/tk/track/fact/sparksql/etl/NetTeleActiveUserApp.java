package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.desttable.FactUserBehaviorClue;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.fact.sparksql.util.DateTimeUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;
/**
 * 网电 月活跃用户app
 * @author itw_shanll
 *
 */
public class NetTeleActiveUserApp implements Serializable {

	private static final long serialVersionUID = -4040598170027896556L;

	public DataFrame getNetTeleActiveUser(HiveContext sqlContext){

		DataFrame df = getMonthActivity(sqlContext);
//		saveAsParquet(df,"/user/tkonline/taikangtrack/data/sll/getMonthActivity");
		
		DataFrame df4 = getFilterEmpty(sqlContext);
//		saveAsParquet(df4, "/user/tkonline/taikangtrack/data/sll/getFilterEmpty");
		
//		DataFrame df5 = getMonthActivityBehaviorTele(sqlContext);
		
		JavaRDD<FactUserBehaviorClue> labelRdd = analysisLabelRDD(df4); 
		DataFrame df3 = sqlContext.createDataFrame(labelRdd, FactUserBehaviorClue.class);
		
//		String fdate = getTodayTime(0);
//		saveAsParquet(df3, "/user/tkonline/taikangtrack/data/app_active_user"+fdate);
		
		return df3;
	}
	
	public DataFrame getUserBehaviorTele(HiveContext sqlContext){
		String hql = "SELECT ROWKEY,user_id,"
				+	"app_type,"
				+	"app_id,"
				+	"event,"
				+	"subtype,"
				+	"label,"
				+	"custom_val,"
				+	"visit_count,"
				+	"from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') as VISIT_TIME,"
				+	"visit_duration,"
				+	"from_id"
				+ " FROM FACT_STATISTICS_EVENT TB1 "
				+ " WHERE TB1.APP_TYPE='app'";
		 if (TK_DataFormatConvertUtil.isExistsPath(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORCLUE_OUTPUTPATH))) {
	    		String timeStamp = Long.toString(DateTimeUtil.getTodayTime(0) / 1000);
	    		String yesterdayTimeStamp = Long.toString(DateTimeUtil.getTodayTime(-1) / 1000);
	    		hql += " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') >= from_unixtime(cast(" + yesterdayTimeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')"
	    			+  " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') < from_unixtime(cast(" + timeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')";
	    	}
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FILTER_COLLECTION");
	}
	
	//  + " unix_timestamp(u.maxtime,'yyyy-MM-dd') >=unix_timestamp('2017-10-18 00:00:00') and unix_timestamp(u.maxtime,'yyyy-MM-dd') < unix_timestamp('2017-10-25 00:00:00') and"
	private DataFrame getMonthActivity(HiveContext sqlContext){
		String timeStamp = getTodayTime(-1);
		String month = getMonth(0);
		String hql = "select u.user_id,cast(u.visitDayCount as string) as visitDayCount,u.lastactivetime from ("
				+ "select uba.user_id, count(distinct uba.nyr) as visitDayCount, max(uba.lastactivetime) as lastactivetime "
				+ " from (select user_id,"
				+ " from_unixtime(cast(clienttime / 1000 as bigint),'yyyy-MM-dd') as nyr,"
				+ " from_unixtime(cast(clienttime / 1000 as bigint),'yyyy-MM-dd hh:mm:ss') as lastactivetime"
				+ " from uba_log_event"
				+ " where user_id is not null "
				+ "        and app_type = 'app'"
				+ "        and from_unixtime(cast(clienttime / 1000 as bigint),'yyyy-MM') = "+"'"+month+"'"+") uba group by uba.user_id) u"
				+ " where "
				+ " u.lastactivetime like "+"'"+timeStamp+"%'"+" and "
				+ " u.visitDayCount = 3";
		
		return DataFrameUtil.getDataFrame(sqlContext, hql, "USER_MONTH_ACTIVETY");
	}
	
	
	//过滤空号
	private DataFrame getFilterEmpty(HiveContext sqlContext){
		String hql = "select a.user_id,"
				+ " fu.name,"
				+ " a.lastactivetime,"
				+ " a.visitdaycount"
				+ " from USER_MONTH_ACTIVETY a"
				+ " inner join P_MEMBER pm"
				+ " on TRIM(a.user_id) = TRIM(pm.member_id)"
				+ " and pm.member_id <> '' and pm.member_id is not null and pm.LOGIN_MOBILEVERIFY='0' "
				+ " left join FACT_USERINFO fu on "
				+ " TRIM(a.user_id) = TRIM(fu.member_id)";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "MEM_FILTER_EMPTY");
	}
	

	public DataFrame getMonthActivityBehaviorTele(HiveContext sqlContext){
		String hql = "SELECT '' as ROWKEY,"
				+ " '' as FROM_ID,"
				+ " 'statisticclue' as APP_TYPE,"
				+ " 'app_logincount' as APP_ID,"
				+ " me.user_id as USER_ID,"
				+ " 'MEM' as USER_TYPE,"
				+ " '2' as EVENT_TYPE,"
				+ " 'app月度活跃' as EVENT,"
				+ " '3次以上' as SUB_TYPE,"
				+ " '' as VISIT_DURATION,"
				+ " 'statistic' as PAGE_TYPE,"
				+ " '制式化营销' as FIRST_LEVEL,"
				+ " '2017销售流程' as SECOND_LEVEL,"
				+ " 'App月活客户' as THIRD_LEVEL,"
				+ " 'App月活3次客户' as FOURTH_LEVEL,"
				+ " me.lastactivetime as VISIT_TIME,"
				+ " visitDayCount as VISIT_COUNT,"
				+ " '' as CLUE_TYPE,"
				+ " 'me.name' as user_name,"
				+ " concat(me.user_id,';',NVL(AU.NAME,'--'),';',me.ACTIVEMONTH,me.LASTACTIVETIME) as REMARK"
				+ " from MEM_FILTER_EMPTY me";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "APP_ACTIVE_USER_BEHAVIOR");
	}
	
	/**
	 * 获取日期
	 * @param day
	 * @return
	 */
	public static String getTodayTime(int day){
		Date date = new Date();
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(date);
		calendar.add(Calendar.DATE, day);
		date=calendar.getTime();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		String dateString = formatter.format(date);
		return dateString;
	}
	//获取月份
	public static String getMonth(int month){
		Date date = new Date();
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(date);
		calendar.add(Calendar.MONTH, month);
		date=calendar.getTime();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM");
		String dateString = formatter.format(date);
		return dateString;
	}
	
	public void saveAsParquet(DataFrame df, String path) {
		TK_DataFormatConvertUtil.deletePath(path);
		df.saveAsParquetFile(path);
	}

	public static JavaRDD<FactUserBehaviorClue> analysisLabelRDD(DataFrame df) {
		JavaRDD<Row> jRDD = df.select("USER_ID", "NAME","LASTACTIVETIME","VISITDAYCOUNT").rdd().toJavaRDD();
		JavaRDD<FactUserBehaviorClue> pRDD = jRDD.map(new Function<Row, FactUserBehaviorClue>() {
			
			private static final long serialVersionUID = -8170582433458947914L;
			
			public FactUserBehaviorClue call(Row row) throws Exception {
				String USER_ID = row.getString(0);
				String NAME = row.getString(1);
				String LASTACTIVETIME = row.getString(2);
				String ACTIVEMONTH = NetTeleActiveUserApp.getMonth(0);
				String VISITDAYCOUNT = row.getString(3);
//				String USER_ID, String NAME, String ACTIVEMONTH, String LASTACTIVETIME,String VISITDAYCOUNT
				return SrcLogParse.analysisNetMonthActive(USER_ID, NAME, ACTIVEMONTH, LASTACTIVETIME,VISITDAYCOUNT);
			}
		});
    	return pRDD;
    }
	
	
	public JavaRDD<FactUserBehaviorClue> getJavaRDD(DataFrame df){
		JavaRDD<Row> jRDD = df.select("ROWKEY", "USER_ID", "USER_TYPE", "APP_TYPE", "APP_ID", 
    			"EVENT_TYPE", "EVENT", "SUB_TYPE", "VISIT_DURATION", "FROM_ID", "PAGE_TYPE", 
    			"FIRST_LEVEL", "SECOND_LEVEL", "THIRD_LEVEL", "FOURTH_LEVEL", "VISIT_TIME","VISIT_COUNT","CLUE_TYPE","REMARK","USER_NAME").rdd().toJavaRDD();
		JavaRDD<FactUserBehaviorClue> pRDD = jRDD.map(new Function<Row, FactUserBehaviorClue>(){

			private static final long serialVersionUID = -1619903863589066084L;
			
			@Override
			public FactUserBehaviorClue call(Row row) throws Exception {
				String ROWKEY = row.getString(0);         
			    String USER_ID = row.getString(1);        
			    String USER_TYPE = row.getString(2);      
			    String APP_TYPE = row.getString(3);       
			    String APP_ID = row.getString(4);         
			    String EVENT_TYPE = row.getString(5);     
			    String EVENT = row.getString(6);     	   
			    String SUB_TYPE = row.getString(7);       
			    String VISIT_DURATION = row.getString(8); 
			    String FROM_ID = row.getString(9);        
			    String PAGE_TYPE = row.getString(10);      
			    String FIRST_LEVEL = row.getString(11);    
			    String SECOND_LEVEL = row.getString(12);   
			    String THIRD_LEVEL = row.getString(13);    
			    String FOURTH_LEVEL = row.getString(14);   
			    String VISIT_TIME = row.getString(15);     
			    String VISIT_COUNT = row.getString(16);     
			    String CLUE_TYPE = row.getString(17);     
			    String REMARK = row.getString(18);	
			    String USER_NAME = row.getString(19);
				return new FactUserBehaviorClue(ROWKEY, USER_ID, USER_TYPE, 
                		APP_TYPE, APP_ID, EVENT_TYPE, EVENT, SUB_TYPE, 
                		VISIT_DURATION, FROM_ID, PAGE_TYPE, FIRST_LEVEL, 
                		SECOND_LEVEL, THIRD_LEVEL, FOURTH_LEVEL, VISIT_TIME, 
                		VISIT_COUNT, CLUE_TYPE, REMARK, USER_NAME);
			}
		});
		return pRDD;
	}
	
	public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<FactUserBehaviorClue> rdd, String path) {
		if (TK_DataFormatConvertUtil.isExistsPath(path)) {
			sqlContext.createDataFrame(rdd, FactUserBehaviorClue.class).save(path, "parquet", SaveMode.Append);
		} else {
			sqlContext.createDataFrame(rdd, FactUserBehaviorClue.class).saveAsParquetFile(path);
		}
	}
}


