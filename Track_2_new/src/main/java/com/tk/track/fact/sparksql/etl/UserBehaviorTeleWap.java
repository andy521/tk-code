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
import com.tk.track.fact.sparksql.desttable.TempUserBehaviorTeleWap;
import com.tk.track.fact.sparksql.main.App;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

/**
 * @ClassName: UserBehaviorTeleWap
 * @Description: TODO
 * @author liran
 * @date 2016年7月14日
 */
public class UserBehaviorTeleWap implements Serializable {
	
	private static final long serialVersionUID = 5241589436279973504L;
	
    String sca_ol = TK_DataFormatConvertUtil.getSchema();

    public DataFrame getUserBehaviorTeleDF(HiveContext sqlContext, String appids) {
    	
    	if(appids==null || appids.equals("")){
    		return null;
    	}
    	
    	sqlContext.load(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH)).registerTempTable("FACT_STATISTICS_EVENT");
    	DataFrame teleDf = getUserBehaviorTeleDF1(sqlContext, appids);
    	
        JavaRDD<TempUserBehaviorTeleWap> labelRdd= analysisLabelRDD(teleDf);
        sqlContext.createDataFrame(labelRdd, TempUserBehaviorTeleWap.class).registerTempTable("TMP_ANALYSISLABEL");
        
        getUserBehaviorTeleDF2(sqlContext);
        
        mergeLRTID(sqlContext);
        getUserBehaviorTeleDF3(sqlContext);
        
        getFACT_USERINFO(sqlContext);
        getUserBehaviorTeleDF4(sqlContext);
        loadPolicyInfoResultTable(sqlContext);
        getUserBehaviorTeleDF5(sqlContext);
        return getUserBehaviorTeleDF6(sqlContext);
    }
    
    public DataFrame getUserBehaviorTeleDF1(HiveContext sqlContext, String appids) {
        String hql = "SELECT * "
                + " FROM  FACT_STATISTICS_EVENT TB1 " 
                + " WHERE LOWER(TB1.APP_TYPE)='h5' and LOWER(TB1.APP_ID) in (" + appids.toLowerCase() + ")"
                + " AND   event <> 'page.load' AND event <> 'page.unload' ";
    	if (TK_DataFormatConvertUtil.isExistsPath(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORTELE_OUTPUTPATH))) {
    		String timeStamp = Long.toString(getTodayTime(0) / 1000);
    		String yesterdayTimeStamp = Long.toString(getTodayTime(-1) / 1000);
    		hql += " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') >= from_unixtime(cast(" + yesterdayTimeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')"
    			+  " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') < from_unixtime(cast(" + timeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')";
    	}
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FILTER_COLLECTION");
    }

	public static JavaRDD<TempUserBehaviorTeleWap> analysisLabelRDD(DataFrame df) {
    	JavaRDD<Row> jRDD = df.select("ROWKEY", "USER_ID", "APP_TYPE", "APP_ID", 
    			"EVENT", "SUBTYPE", "LABEL", "CUSTOM_VAL", "VISIT_COUNT", "VISIT_TIME", "VISIT_DURATION","FROM_ID").rdd().toJavaRDD();
    	JavaRDD<TempUserBehaviorTeleWap> pRDD = jRDD.map(new Function<Row, TempUserBehaviorTeleWap>() {

			private static final long serialVersionUID = -8170582433458947914L;

			public TempUserBehaviorTeleWap call(Row v1) throws Exception {
				return SrcLogParse.analysisWapLable(v1.getString(0), v1.getString(1), v1.getString(2),
				         v1.getString(3), v1.getString(4), v1.getString(5), v1.getString(6),
				         v1.getString(7), v1.getString(8), v1.getString(9), v1.getString(10),v1.getString(11));
			}
    	});
    	return pRDD;
    }
	
    public DataFrame getUserBehaviorTeleDF2(HiveContext sqlContext) {
        String hql = "SELECT TMP_A.ROWKEY, "
        		+ "          TMP_A.FROM_ID, "
        		+ "          TMP_A.APP_TYPE, "
        		+ "          TMP_A.APP_ID, "
        		+ "          TMP_A.USER_ID, "
                + "          TMP_A.USER_TYPE, "
                + "          TMP_A.USER_EVENT, "
                + "          TMP_A.THIRD_LEVEL as THIRDLEVEL, "
                + "          TMP_A.FOURTH_LEVEL as FOURTHLEVEL, "
                + "          TMP_A.LRT_ID as LRT_ID, "
                + "          TMP_A.CLASS_ID, "
                + "          TMP_A.APP_ID as INFOFROM, "
                + "          TMP_A.VISIT_COUNT, "
                + "          TMP_A.VISIT_TIME, "
                + "          TMP_A.VISIT_DURATION, "
                + "          TMP_A.CLASSIFYNAME as CLASSIFY_NAME, "
                + "          TMP_A.LRT_NAME as PRODUCT_NAME "
                + "   FROM TMP_ANALYSISLABEL TMP_A "
                + "  WHERE TMP_A.USER_TYPE <>''"
                + "    AND TMP_A.USER_TYPE IS NOT NULL";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_USER_BEHAVIOR_TELE");
    }    
    
    public DataFrame mergeLRTID(HiveContext sqlContext) {
        String hql = "SELECT    min(TMP_BT.ROWKEY) as ROWKEY, "
        		+ "             TMP_BT.FROM_ID, "
        		+ "             TMP_BT.APP_TYPE, "
        		+ "             TMP_BT.APP_ID, "
        		+ "             TMP_BT.USER_ID, "
        		+ "             TMP_BT.USER_TYPE, "
        		+ "             TMP_BT.USER_EVENT, "
        		+ "             TMP_BT.THIRDLEVEL, "
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
                + "   GROUP BY  TMP_BT.FROM_ID, TMP_BT.APP_TYPE, TMP_BT.APP_ID, TMP_BT.USER_ID,"
                + "             TMP_BT.USER_TYPE, TMP_BT.USER_EVENT, TMP_BT.THIRDLEVEL, TMP_BT.FOURTHLEVEL, TMP_BT.LRT_ID, "
                + "             TMP_BT.CLASS_ID, TMP_BT.INFOFROM ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_USER_BEHAVIOR_MERGE_LRTID");
    }
    
    public DataFrame getUserBehaviorTeleDF3(HiveContext sqlContext) {
        String hql = "SELECT DISTINCT TMP_UBT.ROWKEY, "
        		+ "			 TMP_UBT.FROM_ID,"
        		+ "          TMP_UBT.USER_ID, "
                + "          TMP_UBT.USER_TYPE, " 
                + "          TMP_UBT.USER_EVENT, "
                + "          TMP_UBT.THIRDLEVEL, "
                + "          TMP_UBT.FOURTHLEVEL, "
                + "          TMP_UBT.LRT_ID, "
                + "          TMP_UBT.CLASS_ID, "
                + "          TMP_UBT.INFOFROM, "
                + "          TMP_UBT.VISIT_COUNT, "
                + "          from_unixtime(cast(cast(TMP_UBT.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') as VISIT_TIME, "
                + "          TMP_UBT.VISIT_DURATION, "
                + "          TMP_UBT.CLASSIFY_NAME, "
                + "          TMP_UBT.PRODUCT_NAME "
                + "   FROM   TMP_USER_BEHAVIOR_MERGE_LRTID TMP_UBT ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "USER_BEHAVIOR_FOR_TELE");
    }
    
    public DataFrame getFACT_USERINFO(HiveContext sqlContext) {
        String hql = "SELECT  TFU.NAME, "
        		+ "           TFU.CUSTOMER_ID,"
        		+ "           TFU.MEMBER_ID, "
        		+ "           TFU.OPEN_ID "
        		+ "   FROM    FACT_USERINFO TFU "
        		+ "   left join tkoldb.wx_openid_nwq pc on pc.openid = TFU.OPEN_ID"
        		+ "   WHERE   TFU.CUSTOMER_ID <> '' "
        		+ "   AND     TFU.CUSTOMER_ID IS NOT NULL "
        		+ "   AND     TFU.MEMBER_ID <>'' "
        		+ "   AND     TFU.MEMBER_ID IS NOT NULL"
		        + "   AND     TFU.OPEN_ID <>'' "
		        + "   AND     TFU.OPEN_ID IS NOT NULL"
		        + "   and pc.openid is null";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_USERINFO_CUSTOMER_NO_NULL");
    }

    public DataFrame getUserBehaviorTeleDF4(HiveContext sqlContext) {
        String hql = "SELECT    DISTINCT "
        		+ "             T1.ROWKEY,"
        		+ "				T1.FROM_ID, "
        		+ "             CASE WHEN T1.USER_TYPE='MEM' THEN T1.USER_ID"
        		+ "					 WHEN T1.USER_TYPE='WE' THEN T2.MEMBER_ID"
        		+ "				END AS USER_ID,"	
        		+ "             CASE WHEN T1.USER_TYPE='MEM' THEN 'MEM'"
        		+ "					 WHEN T1.USER_TYPE='WE' THEN 'WE-MEM'"
        		+ "             END AS USER_TYPE, "
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
        		+ "   ON        (T1.USER_TYPE = 'MEM' OR T1.USER_TYPE='WE') "
        		+ "   AND       (T1.USER_ID = T2.MEMBER_ID OR T1.USER_ID=T2.OPEN_ID) ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "USER_BEHAVIOR_FOR_MON_CUSTOMER");
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
    
    public DataFrame getUserBehaviorTeleDF5(HiveContext sqlContext) {
        String hql = "SELECT  DISTINCT"
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
        		+ "           CASE "
        		+ "               WHEN T4.LRT_ID IS NOT NULL AND T4.LRT_ID <> '' "
        		+ "               THEN     "
        		+ "                   '0'  "
        		+ "               ELSE     "
        		+ "                   '1'  "
        		+ "           END AS IF_PAY"
        		+ "   FROM    USER_BEHAVIOR_FOR_MON_CUSTOMER T3  "
        		+ "   LEFT JOIN POLICYINFORESULT T4 "
        		+ "   ON      T4.POLICYHOLDER_ID = T3.CUSTOMER_ID "
        		+ "   AND     T4.LRT_ID = T3.LRT_ID ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_USER_BEHAVIOR_TELE_CUSTORM"); 
    }
    
    public DataFrame getUserBehaviorTeleDF6(HiveContext sqlContext) {
        String hql = "SELECT  MIN(T5.ROWKEY) as ROWKEY, "
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
        		+ "           MIN(T5.IF_PAY) as IF_PAY,"
        		+ "			  T5.FROM_ID,"
        		+ "			  '' AS REMARK,"
        		+ "			  '' AS CLUE_TYPE"
        		+ "  FROM     FACT_USER_BEHAVIOR_TELE_CUSTORM T5  "
        		+ "  WHERE    T5.USER_ID IS NOT NULL "
        		+ "  AND      T5.USER_ID <> ''"
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
    			"VISIT_DURATION", "CLASSIFY_NAME", "PRODUCT_NAME", "USER_NAME", "IF_PAY","FROM_ID","REMARK","CLUE_TYPE").rdd().toJavaRDD();
        JavaRDD<FactUserBehaviorTele> pRDD = jRDD.filter(new Function<Row, Boolean>() {

			private static final long serialVersionUID = 8478336328218391417L;

			public Boolean call(Row v1) throws Exception {
				if ((v1.getString(4) == null) || v1.getString(4).equals("") || v1.getString(4).isEmpty() ||
					(v1.getString(5) == null) || v1.getString(5).equals("") || v1.getString(5).isEmpty()) {
					return false;
				}
				return true;
			}
        	
        }).map(new Function<Row, FactUserBehaviorTele>() {

			private static final long serialVersionUID = -1619903863589066084L;

			public FactUserBehaviorTele call(Row v1) throws Exception {
                return new FactUserBehaviorTele(v1.getString(0), v1.getString(1), v1.getString(2), 
                        v1.getString(3), v1.getString(4), v1.getString(5), 
                        v1.getString(6), v1.getString(7), v1.getString(8), 
                        v1.getString(9), v1.getString(10), v1.getString(11), 
                        v1.getString(12), v1.getString(13), v1.getString(14), v1.getString(15),v1.getString(16),v1.getString(17),v1.getString(18));
            }
        });
        return pRDD;
    }
    
	public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<FactUserBehaviorTele> rdd, String path) {
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
