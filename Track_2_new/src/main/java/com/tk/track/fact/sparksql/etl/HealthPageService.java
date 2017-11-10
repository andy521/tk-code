package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;

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
 * @ClassName: HealthPageService 
 * @Description: 健康服务页面
 * @author langxb
 * @date 2016年11月3日
 */
public class HealthPageService implements Serializable {
	
	private static final long serialVersionUID = 5241589436279973504L;
	
    String sca_ol = TK_DataFormatConvertUtil.getSchema();

    public DataFrame getUserBehaviorClueDF(HiveContext sqlContext, String appids) {
    	
    	if(appids==null || appids.equals("")){
    		return null;
    	}
    	
    	sqlContext.load(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH)).registerTempTable("FACT_STATISTICS_EVENT");
    	String paramSql = " AND ((EVENT='门诊挂号首页' OR EVENT = '重疾绿通道首页' OR EVENT = '电话医生首页' OR EVENT = '齿科券首页' OR EVENT = '体检券首页') "
    			         +" OR (EVENT='健康页面' AND (SUBTYPE in ('定点医院','常见疾病','急救手册','常用药')))"
    			         +" OR (TRIM(EVENT)='health' AND TRIM(SUBTYPE) IN ('3','4','5','6','7','9','10','11','12'))"
    			         + ")";
    	DataFrame teleDf = getDataFromStatisticsEventTable(sqlContext, appids, paramSql);
    	JavaRDD<FactUserBehaviorClue> labelRdd = analysisLabelRDD(teleDf);
        sqlContext.createDataFrame(labelRdd, FactUserBehaviorClue.class).registerTempTable("TMP_ANALYSISLABEL");;
        getFACT_USERINFO(sqlContext);
        DataFrame resultDataFram = fullFillUserNameByUserInfoTable(sqlContext);
        return resultDataFram;
    }
    
    public DataFrame fullFillUserNameByUserInfoTable(HiveContext sqlContext) {
    	String hql = "SELECT TMP_A.ROWKEY,"
    			+"       TMP_A.FROM_ID,"
    			+"       TMP_A.APP_TYPE,"
    			+"       TMP_A.APP_ID,"
    			+"       TMP_A.USER_ID,"
    			+"       TMP_A.USER_TYPE,"
    			+"       TMP_A.EVENT_TYPE,"
    			+"       TMP_A.EVENT,"
    			+"       TMP_A.SUB_TYPE,"
    			+"       TMP_A.VISIT_DURATION,"
    			+"       UM.NAME AS USER_NAME,"
    			+"       TMP_A.PAGE_TYPE,"
    			+"       TMP_A.FIRST_LEVEL,"
    			+"       TMP_A.SECOND_LEVEL,"
    			+"       TMP_A.THIRD_LEVEL,"
    			+"       TMP_A.FOURTH_LEVEL,"
    			+"       TMP_A.VISIT_TIME,"
    			+"       TMP_A.VISIT_COUNT,"
    			+"       TMP_A.CLUE_TYPE,"
    			+"       CONCAT('姓名：',"
    			+"              NVL(UM.NAME, ''),"
    			+"              '\073性别：',"
    			+"              CASE"
    			+"                WHEN UM.GENDER = '0' THEN"
    			+"                 '男'"
    			+"                WHEN UM.GENDER = '1' THEN"
    			+"                 '女'"
    			+"                ELSE"
    			+"					''"
    			+"              END,"
    			+"              '\073出生日期：',"
    			+"              NVL(TO_DATE(UM.BIRTHDAY), ''),"
    			+"              '\073首次访问时间：',"
    			+"              NVL(TMP_A.VISIT_TIME, ''),"
    			+"              '\073访问次数：',"
    			+"              NVL(TMP_A.VISIT_COUNT, ''),"
    			+"              '\073访问时长：',"
    			+"              NVL(TMP_A.VISIT_DURATION, '')) AS REMARK"
    			+"  FROM TMP_ANALYSISLABEL TMP_A"
    			+"  JOIN FACT_USERINFO_CUSTOMER_NO_NULL UM ON TMP_A.USER_ID = UM.MEMBER_ID";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_USER_BEHAVIOR_CLUE");
    }
    
    public DataFrame getDataFromStatisticsEventTable(HiveContext sqlContext, String appids, String paramSql) {
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
                + " WHERE LOWER(TB1.APP_ID) in (" + appids.toLowerCase() + ")"
                + " AND event <> 'page.load' AND event <> 'page.unload' ";
    	if(paramSql != null && !paramSql.isEmpty()) {
    		hql = hql + paramSql;
    	}
        if (TK_DataFormatConvertUtil.isExistsPath(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORCLUE_OUTPUTPATH))) {
    		String timeStamp = Long.toString(DateTimeUtil.getTodayTime(0) / 1000);
    		String yesterdayTimeStamp = Long.toString(DateTimeUtil.getTodayTime(-1) / 1000);
    		hql += " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') >= from_unixtime(cast(" + yesterdayTimeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')"
    			+  " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') < from_unixtime(cast(" + timeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')";
    	}
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FILTER_COLLECTION");
    }

	public static JavaRDD<FactUserBehaviorClue> analysisLabelRDD(DataFrame df) {
		JavaRDD<Row> jRDD = df.select("ROWKEY", "USER_ID", "APP_TYPE", "APP_ID", 
				"EVENT", "SUBTYPE", "LABEL", "CUSTOM_VAL", "VISIT_COUNT", "VISIT_TIME", "VISIT_DURATION","FROM_ID").rdd().toJavaRDD();
		JavaRDD<FactUserBehaviorClue> pRDD = jRDD.map(new Function<Row, FactUserBehaviorClue>() {
			
			private static final long serialVersionUID = -8170582433458947914L;
			
			public FactUserBehaviorClue call(Row row) throws Exception {
				String ROWKEY = row.getString(0);
				String USER_ID = row.getString(1);
				String APP_TYPE = row.getString(2);
				String APP_ID = row.getString(3);
				String EVENT = row.getString(4);
				String SUBTYPE = row.getString(5);
				String LABEL = row.getString(6);
				String CUSTOM_VAL = row.getString(7);
				String VISIT_COUNT = row.getString(8);
				String VISIT_TIME = row.getString(9);
				String VISIT_DURATION = row.getString(10);
				String FROM_ID = row.getString(11);
				return SrcLogParse.analysisHealthServiceLable(ROWKEY, USER_ID, APP_TYPE,
						APP_ID, EVENT, SUBTYPE, LABEL, CUSTOM_VAL, VISIT_COUNT, VISIT_TIME, 
						VISIT_DURATION,FROM_ID);
			}
		});
    	return pRDD;
    }
    
    public DataFrame getFACT_USERINFO(HiveContext sqlContext) {
        String hql = "SELECT  DISTINCT TFU.NAME, "
        		+ "           TRIM(TFU.MEMBER_ID) MEMBER_ID, "
        		+ "           TFU.BIRTHDAY,"
        		+ "			  TFU.GENDER "
        		+ "    FROM   FACT_USERINFO TFU "
        		+ "   WHERE   TFU.MEMBER_ID <>'' "
        		+ "   AND     TFU.MEMBER_ID IS NOT NULL";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_USERINFO_CUSTOMER_NO_NULL");
    }

    public JavaRDD<FactUserBehaviorClue> getJavaRDD(DataFrame df) {
    	JavaRDD<Row> jRDD = df.select("ROWKEY", "USER_ID", "USER_TYPE", "APP_TYPE", "APP_ID", 
    			"EVENT_TYPE", "EVENT", "SUB_TYPE", "VISIT_DURATION", "FROM_ID", "PAGE_TYPE", 
    			"FIRST_LEVEL", "SECOND_LEVEL", "THIRD_LEVEL", "FOURTH_LEVEL", "VISIT_TIME","VISIT_COUNT","CLUE_TYPE","REMARK","USER_NAME").rdd().toJavaRDD();
        JavaRDD<FactUserBehaviorClue> pRDD = jRDD.map(new Function<Row, FactUserBehaviorClue>() {

			private static final long serialVersionUID = -1619903863589066084L;

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
