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
import com.tk.track.fact.sparksql.desttable.FactUserBehaviorTele;
import com.tk.track.fact.sparksql.desttable.TempUserBehaviorTeleWap;
import com.tk.track.fact.sparksql.main.App;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

/**
 * @ClassName: UserBehaviorTeleWechatInsureflow
 * @Description: TODO
 * @author 
 * @date 2017年5月11日
 */
public class UserBehaviorH5Insureflow implements Serializable {
	
	private static final long serialVersionUID = 5241589436279973504L;
	
    String sca_ol = TK_DataFormatConvertUtil.getSchema();

    public DataFrame getUserBehaviorH5DF(HiveContext sqlContext, String appids) {
    	if(appids==null || appids.equals("")){
    		return null;
    	}
    	sqlContext.load(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH)).registerTempTable("FACT_STATISTICS_EVENT");
    	getUserBehaviorH5DF1(sqlContext, appids);
    	//过滤sales_sms表数据
    	getSalesSmsFilter(sqlContext);
    	//关联scode,填充 user_type和user_id
    	getUserBehaviorH5DF2(sqlContext);
    	//关联userinfo表，填充 user_name MobileAccountWorthSearchService 50
    	DataFrame teleDf = fullFillUserNameByUserInfoTable(sqlContext);
    	
        return teleDf;
    }

	public DataFrame getUserBehaviorH5DF1(HiveContext sqlContext, String appids) {
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
	                + " FROM  FACT_STATISTICS_EVENT TB1 "
    			    + " where TB1.app_id in ("+appids+") and TB1.visit_duration>=1000 and TB1.label like '%scode%'";
    	
    	if (TK_DataFormatConvertUtil.isExistsPath(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORTELE_OUTPUTPATH))) {
    		String timeStamp = Long.toString(getTodayTime(0) / 1000);
    		String yesterdayTimeStamp = Long.toString(getTodayTime(-1) / 1000);
    		hql += " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') >= from_unixtime(cast(" + yesterdayTimeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')"
    			+  " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') < from_unixtime(cast(" + timeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')";
    	}
    	return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FILTER_COLLECTION");
    }
	
	 private DataFrame getSalesSmsFilter(HiveContext sqlContext) {
		 String hql = "select * from tkoldb.SALES_SMS S1 where S1.act_id=3961";
//		 			+ " inner join (select max(S2.act_id) max_id from tkoldb.SALES_SMS S2 where S2.act_id like '3961%') b on S1.act_id = b.max_id";
		 
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
                + "          '武汉营销数据' FIRST_LEVEL, "
                + "          '武汉日常营销类' SECOND_LEVEL, "
                + "          '健康1+1推荐页面长时间打开' THIRD_LEVEL, "
                + "          '泰康e享健康保障计划' FOURTH_LEVEL, "
                + "          '2' CLUE_TYPE, "
                + "          ''  REMARK "
                + "     FROM TMP_FILTER_COLLECTION TMP_A "
                + "     JOIN TMP_SALES_SMS UM"
                + "       ON TMP_A.USER_ID = UM.SALES_SMS_CODE";
        
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_USER_BEHAVIOR_SCODE");
    }
    
    public DataFrame fullFillUserNameByUserInfoTable(HiveContext sqlContext) {
    	String hql = "SELECT /*+MAPJOIN(TMP_A)*/"     
				+ "			 TMP_A.ROWKEY, "
				+ "			 TMP_A.USER_ID, "
				+ "			 TMP_A.USER_TYPE, "
        		+ "          TMP_A.APP_TYPE, "
        		+ "          TMP_A.APP_ID, "
        		+ "          TMP_A.EVENT, "
        		+ "          TMP_A.EVENT_TYPE, "
                + "          TMP_A.SUB_TYPE, "
                + "          TMP_A.VISIT_COUNT, "
                + "          TMP_A.VISIT_TIME, "
                + "          TMP_A.VISIT_DURATION, "
                + "          TMP_A.FROM_ID, "
                + "          TMP_A.PAGE_TYPE, "
                + "          TMP_A.FIRST_LEVEL, "
                + "          TMP_A.SECOND_LEVEL, "
                + "          TMP_A.THIRD_LEVEL, "
                + "          TMP_A.FOURTH_LEVEL, "
                + "          TMP_A.CLUE_TYPE, "
                + "          UM.NAME AS USER_NAME, "
                + "          TMP_A.REMARK "
                + "     FROM TMP_USER_BEHAVIOR_SCODE TMP_A "
                + "     JOIN FACT_USERINFO UM ON (TMP_A.USER_ID = UM.MEMBER_ID AND UM.MEMBER_ID IS NOT NULL AND UM.MEMBER_ID <> '') "
                + "     OR (TMP_A.USER_ID = UM.CUSTOMER_ID AND UM.CUSTOMER_ID IS NOT NULL AND UM.CUSTOMER_ID <> '') "
                + "     OR (TMP_A.USER_ID = UM.MOBILE AND UM.MOBILE IS NOT NULL AND UM.MOBILE <> '') ";
    	
        return DataFrameUtil.getDataFrame(sqlContext, hql, "USER_BEHAVIOR_SCODE_CLUE");
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


    public JavaRDD<FactUserBehaviorClueScode> getJavaRDD(DataFrame df) {
    	JavaRDD<Row> jRDD = df.select("ROWKEY", "USER_ID", "USER_TYPE", "APP_TYPE", "APP_ID", 
    	"EVENT", "EVENT_TYPE", "SUB_TYPE", "VISIT_COUNT", "VISIT_TIME", "VISIT_DURATION", 
    	"FROM_ID", "PAGE_TYPE", "FIRST_LEVEL", "SECOND_LEVEL", "THIRD_LEVEL","FOURTH_LEVEL","CLUE_TYPE","USER_NAME","REMARK").rdd().toJavaRDD();
        JavaRDD<FactUserBehaviorClueScode> pRDD = jRDD.map(new Function<Row, FactUserBehaviorClueScode>() {

			private static final long serialVersionUID = -1619903863589066084L;

			public FactUserBehaviorClueScode call(Row v1) throws Exception {
                return new FactUserBehaviorClueScode(v1.getString(0), v1.getString(1), v1.getString(2), 
                        v1.getString(3), v1.getString(4), v1.getString(5), 
                        v1.getString(6), v1.getString(7), v1.getString(8), 
                        v1.getString(9), v1.getString(10), v1.getString(11), 
                        v1.getString(12), v1.getString(13), v1.getString(14), 
                        v1.getString(15),v1.getString(16),v1.getString(17),
                        v1.getString(18),v1.getString(19));
            }
        });
        return pRDD;
    }
    
	public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<FactUserBehaviorClueScode> rdd, String path) {
		if (TK_DataFormatConvertUtil.isExistsPath(path)) {
			sqlContext.createDataFrame(rdd, FactUserBehaviorClueScode.class).repartition(400).save(path, "parquet", SaveMode.Append);
		} else {
			sqlContext.createDataFrame(rdd, FactUserBehaviorClueScode.class).repartition(400).saveAsParquetFile(path);
		}
	}
    
}
