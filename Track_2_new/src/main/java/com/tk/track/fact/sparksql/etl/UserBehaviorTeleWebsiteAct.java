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
import com.tk.track.fact.sparksql.desttable.FactUserBehaviorTele;
import com.tk.track.fact.sparksql.desttable.TempUserBehaviorTele;
import com.tk.track.fact.sparksql.main.App;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

/**
 * @ClassName: UserBehaviorTeleWechat
 * @Description: TODO
 * @author liran
 * @date 2016年7月5日
 */
public class UserBehaviorTeleWebsiteAct implements Serializable {

	private static final long serialVersionUID = 7596157039547896085L;

	String sca_ol = TK_DataFormatConvertUtil.getSchema();

	public DataFrame getUserBehaviorTeleDF(HiveContext sqlContext ,String appids) {
		
		if(appids==null || appids.equals("")){
			return null;
		}
		
		sqlContext.load(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH)).registerTempTable("FACT_STATISTICS_EVENT");
		DataFrame teleDf = getUserBehaviorTeleDF1(sqlContext, appids);
		
		JavaRDD<TempUserBehaviorTele> labelRdd = analysisLabelRDD(teleDf);
		sqlContext.createDataFrame(labelRdd, TempUserBehaviorTele.class).registerTempTable("TMP_ANALYSISLABEL");

		getUserBehaviorTeleDF2(sqlContext);
		mergeLRTID(sqlContext);
		getUserBehaviorTeleDF3(sqlContext);

		getFACT_USERINFO(sqlContext);
		getUserBehaviorTeleDF4(sqlContext);
		getUserBehaviorTeleDF5(sqlContext);
		return getUserBehaviorTeleDF6(sqlContext);
	}

	
	public DataFrame getUserBehaviorTeleDF1(HiveContext sqlContext ,String appids) {
		String hql = "SELECT * "
					+ " FROM  FACT_STATISTICS_EVENT TB1 "
					+ " WHERE LOWER(TB1.APP_TYPE)='website' and LOWER(TB1.APP_ID) in (" + appids.toLowerCase() + ")"
					+ " AND   (TRIM(TB1.SUBTYPE) = '' OR TB1.SUBTYPE IS NULL) "//二期新增（只取页面加载、不取点击）
					+ " AND   LOWER(TB1.EVENT) <> 'page.load' AND LOWER(TB1.EVENT) <> 'page.unload' ";
		if (TK_DataFormatConvertUtil.isExistsPath(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORTELE_OUTPUTPATH))) {
			String timeStamp = Long.toString(getTodayTime(0) / 1000);
			String yesterdayTimeStamp = Long.toString(getTodayTime(-1) / 1000);
			hql += 	" AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') >= from_unixtime(cast("
					+ yesterdayTimeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')"
					+ " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') < from_unixtime(cast("
					+ timeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')";
		}
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FILTER_COLLECTION");
	}

	public static JavaRDD<TempUserBehaviorTele> analysisLabelRDD(DataFrame df) {
		JavaRDD<Row> jRDD = df.select("ROWKEY", "USER_ID", "APP_TYPE", "APP_ID", "EVENT", "SUBTYPE", 
						"LABEL", "CUSTOM_VAL", "VISIT_COUNT", "VISIT_TIME", "VISIT_DURATION", "FROM_ID").rdd().toJavaRDD();
		JavaRDD<TempUserBehaviorTele> pRDD = jRDD.map(new Function<Row, TempUserBehaviorTele>() {

					private static final long serialVersionUID = -3703145392781658635L;

					public TempUserBehaviorTele call(Row v1) throws Exception {
						// TODO Auto-generated method stub
						return SrcLogParse.analysislShopLable(v1.getString(0),
								v1.getString(1), v1.getString(2),v1.getString(3), v1.getString(4),
								v1.getString(5), v1.getString(6),v1.getString(7), v1.getString(8),
								v1.getString(9), v1.getString(10),v1.getString(11));
					}
				});
		return pRDD;
	}

	public DataFrame getUserBehaviorTeleDF2(HiveContext sqlContext) {
		String hql = "SELECT TMP_A.ROWKEY, "
				+ "          TMP_A.FROM_ID, "
				+ "          TMP_A.APP_TYPE, "
				+ "          TMP_A.APP_ID, "
				+ "          TMP_A.CLASS_NAME, "//二期修改
				+ "          TMP_A.USER_ID, " 
				+ "          'MEM' as USER_TYPE, "
				+ "          TMP_A.USER_EVENT, "
                + "          '' as FOURTHLEVEL, "//二期修改
				+ "          '' as LRT_ID, "//二期修改
				+ "          TMP_A.CLASS_ID, "//二期修改
				+ "          TMP_A.APP_ID as INFOFROM, "//二期修改（直接取APP_ID）
				+ "          TMP_A.VISIT_COUNT, "
				+ "          TMP_A.VISIT_TIME, "
				+ "          TMP_A.VISIT_DURATION, "
				+ "          TMP_A.CLASSIFYNAME as CLASSIFY_NAME,"//二期修改
				+ "          TMP_A.PRODUCTNAME as PRODUCT_NAME "//二期修改
				+ "   FROM TMP_ANALYSISLABEL TMP_A ";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_USER_BEHAVIOR_TELE");
	}

	public DataFrame mergeLRTID(HiveContext sqlContext) {
		String hql = "SELECT    min(TMP_BT.ROWKEY) as ROWKEY, "
				+ "             TMP_BT.FROM_ID, "
				+ "             TMP_BT.APP_TYPE, "
				+ "             TMP_BT.APP_ID, "
				+ "             TMP_BT.CLASS_NAME,"
				+ "             TMP_BT.USER_ID, "
				+ "             TMP_BT.USER_TYPE, "
				+ "             TMP_BT.USER_EVENT, "
				+ "             TMP_BT.FOURTHLEVEL, "
				+ "             TMP_BT.LRT_ID, "
				+ "             TMP_BT.CLASS_ID, "
				+ "             TMP_BT.INFOFROM, "
				+ "             sum(TMP_BT.VISIT_COUNT)    as VISIT_COUNT, "
				+ "             min(TMP_BT.VISIT_TIME)     as VISIT_TIME, "
				+ "             sum(TMP_BT.VISIT_DURATION) as VISIT_DURATION, "
				+ "             min(TMP_BT.CLASSIFY_NAME)  as CLASSIFY_NAME, "
				+ "             min(TMP_BT.PRODUCT_NAME)   as PRODUCT_NAME "
				+ "   FROM      TMP_USER_BEHAVIOR_TELE TMP_BT"
				+ "   GROUP BY  TMP_BT.FROM_ID, TMP_BT.APP_TYPE, TMP_BT.APP_ID, TMP_BT.CLASS_NAME,"
				+ "             TMP_BT.USER_ID, TMP_BT.USER_TYPE, TMP_BT.USER_EVENT, TMP_BT.FOURTHLEVEL, TMP_BT.LRT_ID, "
				+ "             TMP_BT.CLASS_ID, TMP_BT.INFOFROM ";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_USER_BEHAVIOR_MERGE_LRTID");
	}

	public DataFrame getUserBehaviorTeleDF3(HiveContext sqlContext) {
		String hql = "SELECT DISTINCT "
				+ "          TMP_UBT.ROWKEY, "
				+ "			 TMP_UBT.FROM_ID,"
				+ "          TMP_UBT.USER_ID, "
				+ "          TMP_UBT.USER_TYPE, "
				+ "          TMP_UBT.USER_EVENT, "
                + "          '' as THIRDLEVEL, "//二期修改
                + "          TMP_UBT.FOURTHLEVEL, "
				+ "          TMP_UBT.LRT_ID, "
				+ "          TMP_UBT.CLASS_ID, "
				+ "          TMP_UBT.INFOFROM, "
				+ "          TMP_UBT.VISIT_COUNT, "
				+ "          from_unixtime(cast(cast(TMP_UBT.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') as VISIT_TIME,"
				+ "          TMP_UBT.VISIT_DURATION, "
				+ "          TMP_UBT.CLASSIFY_NAME, "
				+ "          TMP_UBT.PRODUCT_NAME "
				+ "   FROM   TMP_USER_BEHAVIOR_MERGE_LRTID TMP_UBT";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "USER_BEHAVIOR_FOR_TELE");
	}

	public DataFrame getFACT_USERINFO(HiveContext sqlContext) {
		String hql = "SELECT  TFU.NAME, "
				+ "           TFU.CUSTOMER_ID, "
				+ "           TFU.MEMBER_ID "
				+ "   FROM    FACT_USERINFO TFU "
				+ "   WHERE   TFU.CUSTOMER_ID <> '' "
				+ "   AND     TFU.CUSTOMER_ID IS NOT NULL "
				+ "   AND     TFU.MEMBER_ID <>'' "
				+ "   AND     TFU.MEMBER_ID IS NOT NULL";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_USERINFO_CUSTOMER_NO_NULL", DataFrameUtil.CACHETABLE_PARQUET);
	}

	public DataFrame getUserBehaviorTeleDF4(HiveContext sqlContext) {
		String hql = "SELECT    DISTINCT "
				+ "             T1.ROWKEY,"
				+ "				T1.FROM_ID, "
				+ "             T1.USER_ID, "
				+ "             T1.USER_TYPE, "
				+ "             T1.USER_EVENT, "
        		+ "             T1.THIRDLEVEL, "
        		+ "             T1.FOURTHLEVEL, "
				+ "             T1.LRT_ID, "
				+ "             T1.CLASS_ID, "
				+ "             T1.INFOFROM, "
				+ "             T1.VISIT_COUNT, "
				+ "             T1.VISIT_TIME, "
				+ "             T1.VISIT_DURATION, "
				+ "             T1.CLASSIFY_NAME, "
				+ "             T1.PRODUCT_NAME, "
				+ "             T2.NAME, "
				+ "             T2.CUSTOMER_ID "
				+ "   FROM      USER_BEHAVIOR_FOR_TELE T1 "
				+ "   LEFT JOIN FACT_USERINFO_CUSTOMER_NO_NULL T2 "
				+ "   ON        T1.USER_TYPE = 'MEM' "
				+ "   AND       T1.USER_ID = T2.MEMBER_ID ";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "USER_BEHAVIOR_FOR_MON_CUSTOMER");
	}

	public Boolean loadPolicyInfoResultTable(HiveContext sqlContext) {
		boolean succ = false;
		String sysdt = App.GetSysDate(0);
		String pathTemp = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTNETPOLICYINFORESULT_OUTPUTPATH);
		String path = pathTemp + "-" + sysdt;
		if (TK_DataFormatConvertUtil.isExistsPath(path)) {
			succ = true;
			sqlContext.load(path).registerTempTable("POLICYINFORESULT"); // 有效保单
		}
		return succ;
	}

	public DataFrame getUserBehaviorTeleDF5(HiveContext sqlContext) {
		String hql = "SELECT  DISTINCT "
				+ "           T3.ROWKEY, "
				+ "			  T3.FROM_ID,"
				+ "           T3.USER_ID, "
				+ "           T3.USER_TYPE, "
				+ "           T3.USER_EVENT, "
        		+ "           T3.THIRDLEVEL, "
        		+ "           T3.FOURTHLEVEL, "
				+ "           T3.LRT_ID, "
				+ "           T3.CLASS_ID, "
				+ "           T3.INFOFROM, "
				+ "           CAST(T3.VISIT_COUNT as STRING) as VISIT_COUNT, "
				+ "           CAST(T3.VISIT_TIME as STRING) as VISIT_TIME, "
				+ "           CAST(T3.VISIT_DURATION as STRING) as VISIT_DURATION, "
				+ "           T3.CLASSIFY_NAME, "
				+ "           T3.PRODUCT_NAME, "
				+ "           T3.NAME as USER_NAME, "
				+ "           '1' as IF_PAY, "//二期修改，默认未购买
				+ "			  '' as LIA_POLICYNO"
				+ "   FROM    USER_BEHAVIOR_FOR_MON_CUSTOMER T3 ";
		return DataFrameUtil.getDataFrame(sqlContext, hql,"FACT_USER_BEHAVIOR_TELE_CUSTORM",DataFrameUtil.CACHETABLE_PARQUET); // 多个custormID的场合
	}

	public DataFrame getUserBehaviorTeleDF6(HiveContext sqlContext) {
		String hql = "SELECT  min(T5.ROWKEY) as ROWKEY, "
				+ "           T5.USER_ID, "
				+ "           T5.USER_TYPE, "
				+ "           T5.USER_EVENT, "
        		+ "           T5.THIRDLEVEL, "
        		+ "           T5.FOURTHLEVEL, "
				+ "           T5.LRT_ID, "
				+ "           T5.CLASS_ID, "
				+ "           T5.INFOFROM, "
				+ "           T5.VISIT_COUNT, "
				+ "           T5.VISIT_TIME, "
				+ "           T5.VISIT_DURATION, "
				+ "           T5.CLASSIFY_NAME, "
				+ "           T5.PRODUCT_NAME, "
				+ "           T5.USER_NAME, "
				+ "           T5.IF_PAY,"
				+ "			  T5.FROM_ID, "
				+ "			  '' AS REMARK,"
				+ "			  '' AS CLUE_TYPE"
				+ "  FROM     FACT_USER_BEHAVIOR_TELE_CUSTORM T5  "
				+ "  GROUP BY T5.USER_ID,"
				+ "           T5.USER_TYPE, "
				+ "           T5.USER_EVENT, "
        		+ "           T5.THIRDLEVEL, "
        		+ "           T5.FOURTHLEVEL, "
				+ "           T5.LRT_ID, "
				+ "           T5.CLASS_ID, "
				+ "           T5.INFOFROM, "
				+ "           T5.VISIT_COUNT, "
				+ "           T5.VISIT_TIME, "
				+ "           T5.VISIT_DURATION, "
				+ "           T5.CLASSIFY_NAME, "
				+ "           T5.PRODUCT_NAME, "
				+ "           T5.USER_NAME,"
				+ "           T5.IF_PAY,"
				+ "			  T5.FROM_ID";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_USER_BEHAVIOR_TELE");
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

	public JavaRDD<FactUserBehaviorTele> getJavaRDD(DataFrame df) {
    	JavaRDD<Row> jRDD = df.select("ROWKEY", "USER_ID", "USER_TYPE", "USER_EVENT", "THIRDLEVEL", 
    			"FOURTHLEVEL", "LRT_ID", "CLASS_ID", "INFOFROM", "VISIT_COUNT", "VISIT_TIME", 
    			"VISIT_DURATION", "CLASSIFY_NAME", "PRODUCT_NAME", "USER_NAME", "IF_PAY", "FROM_ID","REMARK","CLUE_TYPE").rdd().toJavaRDD();
		JavaRDD<FactUserBehaviorTele> pRDD = jRDD.map(new Function<Row, FactUserBehaviorTele>() {
			
			private static final long serialVersionUID = 6372868694557719480L;

			public FactUserBehaviorTele call(Row v1) throws Exception {
                return new FactUserBehaviorTele(v1.getString(0), v1.getString(1), v1.getString(2), 
                        v1.getString(3), v1.getString(4), v1.getString(5), 
                        v1.getString(6), v1.getString(7), v1.getString(8), 
                        v1.getString(9), v1.getString(10), v1.getString(11), 
                        v1.getString(12), v1.getString(13), v1.getString(14), 
                        v1.getString(15), v1.getString(16),v1.getString(17),v1.getString(18));
			}
		});
		return pRDD;
	}

	public void pairRDD2Parquet(HiveContext sqlContext,JavaRDD<FactUserBehaviorTele> rdd, String path) {
		if (TK_DataFormatConvertUtil.isExistsPath(path)) {
			sqlContext.createDataFrame(rdd, FactUserBehaviorTele.class).save(path, "parquet", SaveMode.Append);
		} else {
			sqlContext.createDataFrame(rdd, FactUserBehaviorTele.class).saveAsParquetFile(path);
		}
	}

	public void saveAsParquet(DataFrame df, String path) {
		TK_DataFormatConvertUtil.deletePath(path);
		df.saveAsParquetFile(path);
	}
}
