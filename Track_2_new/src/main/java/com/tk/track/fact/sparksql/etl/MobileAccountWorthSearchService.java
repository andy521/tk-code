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
import com.tk.track.fact.sparksql.main.App;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.fact.sparksql.util.DateTimeUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

/**
 * @ClassName: AccountWorthSearchService 
 * @Description: 价值账户查询
 * @author langxb
 * @date 2016年9月20日
 */
public class MobileAccountWorthSearchService implements Serializable {
	
	private static final long serialVersionUID = 5241589436279973504L;
	
    String sca_ol = TK_DataFormatConvertUtil.getSchema();

    public DataFrame getUserBehaviorClueDF(HiveContext sqlContext, String appids) {
    	
    	if(appids==null || appids.equals("")){
    		return null;
    	}
    	
    	sqlContext.load(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH)).registerTempTable("FACT_STATISTICS_EVENT");
    	String paramSql = " AND ((trim(EVENT)='money' AND trim(SUBTYPE) in "
    		+" ('3','4','5','6','7','8','9','10','11','14','15','16','17',"
    		+" '18','19','20','22','23','24','25','26','27','28','29','30',"
    		+" '31','32','33','34','35','36','37','38','39','40','41','42',"
    		+" '43','44','45','46')) OR trim(EVENT)='账户价格查询页面首页' OR (trim(EVENT)='净值公布' AND trim(SUBTYPE)='账户查看'))";
    	DataFrame teleDf = getDataFromStatisticsEventTable(sqlContext, appids, paramSql);
    	JavaRDD<FactUserBehaviorClue> labelRdd = analysisMobileLabelRDD(teleDf);
        sqlContext.createDataFrame(labelRdd, FactUserBehaviorClue.class).registerTempTable("TMP_ANALYSISLABEL");;
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
    			+"                 ''"
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
    			+"  JOIN FACT_USERINFO UM ON TMP_A.USER_ID = UM.MEMBER_ID"
    			+" WHERE UM.MEMBER_ID IS NOT NULL AND UM.MEMBER_ID <> ''";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_USER_BEHAVIOR_CLUE");
    }
    /**
     * 
     * @Description: 添加线索类型列（1-线索；2-事件；0-预留）
     * @param sqlContext
     * @return
     * @author moyunqing
     * @date 2016年9月19日
     * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    @SuppressWarnings("unused")
	private DataFrame getUserBHTele(HiveContext sqlContext) {
		String sql = "SELECT TL.*,"
				+ "			 CASE WHEN TLA.USER_ID IS NULL THEN '1'"
				+ "				  WHEN TLA.USER_ID IS NOT NULL THEN '2'"
				+ "				  ELSE '0'"
				+ "			 END AS TELE_TYPE"
				+ "    FROM  TMP_TELE TL"
				+ "	 LEFT JOIN (SELECT DISTINCT "
				+ "					   USER_ID "
				+ "               FROM TMP_TELEALL"
				+ "				 WHERE USER_ID <> ''"
				+ "				   AND USER_ID IS NOT NULL) TLA"
				+ "		 ON  TL.USER_ID = TLA.USER_ID ";
    	return DataFrameUtil.getDataFrame(sqlContext, sql, "FACT_USER_BEHAVIOR_TELE");
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
                + " WHERE TB1.APP_ID in (" + appids + ")"
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
				return SrcLogParse.analysisAccountWorthSearchLable(ROWKEY, USER_ID, APP_TYPE,
						APP_ID, EVENT, SUBTYPE, LABEL, CUSTOM_VAL, VISIT_COUNT, VISIT_TIME, 
						VISIT_DURATION,FROM_ID);
			}
    	});
    	return pRDD;
    }
	
	public static JavaRDD<FactUserBehaviorClue> analysisMobileLabelRDD(DataFrame df) {
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
				return SrcLogParse.analysisMobileAccountWorthSearchLable(ROWKEY, USER_ID, APP_TYPE,
						APP_ID, EVENT, SUBTYPE, LABEL, CUSTOM_VAL, VISIT_COUNT, VISIT_TIME, 
						VISIT_DURATION,FROM_ID);
			}
		});
		return pRDD;
	}
    
    public DataFrame getFACT_USERINFO(HiveContext sqlContext) {
        String hql = "SELECT  TFU.NAME, "
        		+ "           TFU.CUSTOMER_ID,"
        		+ "           TFU.MEMBER_ID "
        		+ "    FROM   FACT_USERINFO TFU "
        		+ "   WHERE   TFU.CUSTOMER_ID <> '' "
        		+ "   AND     TFU.CUSTOMER_ID IS NOT NULL "
        		+ "   AND     TFU.MEMBER_ID <>'' "
        		+ "   AND     TFU.MEMBER_ID IS NOT NULL";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_USERINFO_CUSTOMER_NO_NULL");
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
        		+ "			  '' AS REMARK"
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
        		+ "			  T5.FROM_ID";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_TELE");
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
    
    public void saveAsParquet(DataFrame df, String path) {
		TK_DataFormatConvertUtil.deletePath(path);
		df.saveAsParquetFile(path);
	}
}
