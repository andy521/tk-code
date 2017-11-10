package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.desttable.FactUserBehaviorClueScode;
import com.tk.track.fact.sparksql.desttable.FactUserBehaviorClueScode_bw;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

/**
 * @ClassName: UserBehaviorTeleWechatInsureflow
 * @Description: TODO
 * @author 
 * @date 2017年5月11日
 */
public class UserBehaviorWechatInsureflow_bwMedical implements Serializable {
	
	private static final long serialVersionUID = 5241589436279973504L;
	
    String sca_ol = TK_DataFormatConvertUtil.getSchema();

    public DataFrame getUserBehaviorWechatDF(HiveContext sqlContext, String appids) {
    	if(appids==null || appids.equals("")){
    		return null;
    	}
    	sqlContext.load(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH)).registerTempTable("FACT_STATISTICS_EVENT");
    	getUserBehaviorWechatDF1(sqlContext, appids);
    	//过滤sales_sms表数据
    	getSalesSmsFilter(sqlContext);
    	//关联scode,填充 user_type和user_id
    	
    	//关联userinfo表，填充 user_name MobileAccountWorthSearchService 50
    	DataFrame teleDf =getUserBehaviorH5DF2(sqlContext);
    	
        return teleDf;
    }

	public DataFrame getUserBehaviorWechatDF1(HiveContext sqlContext, String appids) {
		String hql="select rowkey,user_id,\r\n" + 
						 "regexp_extract(label,'scode:\\\"(.*?)\\\"')  as  scode,\r\n" + 
						 "app_type,\r\n" + 
						 "app_id,\r\n" + 
						 "event,\r\n" + 
						 "subtype,\r\n" + 
						 "label,\r\n" + 
						 "custom_val,\r\n" + 
						 "visit_count,\r\n" + 
						 "from_unixtime(cast(cast(tb1.visit_time as bigint) / 1000 as bigint),'yyyy-mm-dd hh:mm:ss') as visit_time,\r\n" + 
						 "visit_duration,\r\n" + 
						 "from_id\r\n" + 
						 "from  FACT_STATISTICS_EVENT tb1  where tb1.app_id  in("+appids+") \r\n" + 
						 "and tb1.from_id in('64068','64069','64070') and tb1.label like '%scode%' and  tb1.label like '%S20170290%' and tb1.event='活动首页' and  tb1.subtype='立即投保'";
    	if (TK_DataFormatConvertUtil.isExistsPath(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORTELE_BWMEDICAL_OUTPUTPATH))) {
    		String timeStamp = Long.toString(getTodayTime(0) / 1000);
    		String yesterdayTimeStamp = Long.toString(getTodayTime(-1) / 1000);
    		hql += " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') >= from_unixtime(cast(" + yesterdayTimeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')"
    			+  " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') < from_unixtime(cast(" + timeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')";
    	}
    	return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FILTER_COLLECTION");
    }
	
	 private DataFrame getSalesSmsFilter(HiveContext sqlContext) {
		 String hql = "select * from tkoldb.SALES_SMS S1 where S1.act_id='4461'";
		 return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_SALES_SMS");
	 }
	 
    public DataFrame getUserBehaviorH5DF2(HiveContext sqlContext) {
                
    	String hql = "SELECT /*+MAPJOIN(TMP_A)*/"
        		+ "         TMP_A.ROWKEY, "
        		+ "         UM.CUSTOMER_DATA USER_ID, "
        		+ "         CASE WHEN UM.CUSTOMER_DATA_TYPE = '0' THEN 'PHO'"
				+ "              WHEN UM.CUSTOMER_DATA_TYPE = '1' THEN 'MEM'"
				+ "              WHEN UM.CUSTOMER_DATA_TYPE = '2' THEN 'CUS'"
				+ "			 END AS USER_TYPE,"
        		+ "          TMP_A.APP_TYPE, "
        		+ "          TMP_A.APP_ID, "
        		+ "          TMP_A.EVENT, "
        		+ "          'load' EVENT_TYPE, "
                + "          TMP_A.SUBTYPE SUB_TYPE, "
                + "          TMP_A.VISIT_COUNT, "
                + "          TMP_A.VISIT_TIME, "
                + "          TMP_A.VISIT_DURATION, "
                + "          TMP_A.FROM_ID, "
                + "          '' PAGE_TYPE, "
                + "          '官网会员行为' FIRST_LEVEL, "
                + "          'Wap产品详情页停留' SECOND_LEVEL, "
                + "          '短信发送线索捕捉' THIRD_LEVEL, "
                + "          '百万医疗' FOURTH_LEVEL, "
                + "          '2' CLUE_TYPE, "
                + "          ''  REMARK "
                + "     FROM TMP_FILTER_COLLECTION TMP_A "
                + "     JOIN TMP_SALES_SMS UM"
                + "     ON TMP_A.SCODE = UM.SALES_SMS_CODE";
        
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_USER_BEHAVIOR_SCODE");
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
    public JavaRDD<FactUserBehaviorClueScode_bw> getJavaRDD(DataFrame df) {
    	JavaRDD<Row> jRDD = df.select("ROWKEY", "USER_ID", "USER_TYPE", "APP_TYPE", "APP_ID", 
    	"EVENT", "EVENT_TYPE", "SUB_TYPE", "VISIT_COUNT", "VISIT_TIME", "VISIT_DURATION", 
    	"FROM_ID", "PAGE_TYPE", "FIRST_LEVEL", "SECOND_LEVEL", "THIRD_LEVEL","FOURTH_LEVEL","CLUE_TYPE","REMARK").rdd().toJavaRDD();
        JavaRDD<FactUserBehaviorClueScode_bw> pRDD = jRDD.map(new Function<Row, FactUserBehaviorClueScode_bw>() {

			private static final long serialVersionUID = -1619903863589066084L;

			public FactUserBehaviorClueScode_bw call(Row v1) throws Exception {
                return new FactUserBehaviorClueScode_bw(v1.getString(0), v1.getString(1), v1.getString(2), 
                        v1.getString(3), v1.getString(4), v1.getString(5), 
                        v1.getString(6), v1.getString(7), v1.getString(8), 
                        v1.getString(9), v1.getString(10), v1.getString(11), 
                        v1.getString(12), v1.getString(13), v1.getString(14), 
                        v1.getString(15),v1.getString(16),v1.getString(17),
                        v1.getString(18));
            }
        });
        return pRDD;
    }
	public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<FactUserBehaviorClueScode_bw> rdd, String path) {
		if (TK_DataFormatConvertUtil.isExistsPath(path)) {
			sqlContext.createDataFrame(rdd, FactUserBehaviorClueScode_bw.class).repartition(400).save(path, "parquet", SaveMode.Append);
		} else {
			sqlContext.createDataFrame(rdd, FactUserBehaviorClueScode_bw.class).repartition(400).saveAsParquetFile(path);
		}
	}
    
}
