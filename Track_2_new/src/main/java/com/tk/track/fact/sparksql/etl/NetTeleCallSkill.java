package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Hashtable;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.desttable.FactNetTeleCallSkill;
import com.tk.track.fact.sparksql.desttable.TempNetTeleCallSkill;
import com.tk.track.fact.sparksql.main.App;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

/**
 * @ClassName: NetTeleCallSkill
 * @Description: TODO
 * @author liran
 * @date 2016年7月26日
 */
public class NetTeleCallSkill implements Serializable {

	private static final long serialVersionUID = -2215354555782665601L;
	
    public DataFrame getNetTeleCallSkillDF(HiveContext sqlContext,String appids) {
    	
    	if(appids==null || appids.equals("")){
    		return null;
    	}
    	
    	sqlContext.load(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_CALLSKILL_OUTPUTPATH)).registerTempTable("FACT_STATISTICS_EVENT");
        DataFrame callSkillDf = getNetTeleCallSkillDF1(sqlContext, appids);
                
        JavaRDD<TempNetTeleCallSkill> labelRDD= analysisAppFieldsRDD(callSkillDf);
        sqlContext.createDataFrame(labelRDD, TempNetTeleCallSkill.class).registerTempTable("TMP_ANALYSISLABEL");
        
        getNetTeleCallSkillDF2(sqlContext);
        getNetTeleCallSkillDF3(sqlContext);
        
        DataFrame df1 = getNetTeleCallSkillDF4(sqlContext);
        return df1;
    }
    
    public DataFrame getNetTeleCallSkillDF1(HiveContext sqlContext, String appids) {
        String hql = "SELECT * "
                + " FROM   FACT_STATISTICS_EVENT TB1 " 
                + " WHERE LOWER(TB1.APP_TYPE)='system' and LOWER(TB1.APP_ID) in ("+ appids.toLowerCase() +")"
                + " AND   event <> 'page.load' AND event <> 'page.unload' ";
    	if (TK_DataFormatConvertUtil.isExistsPath(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_NETTELECALLSKILL_OUTPUTPATH))) {
    		String timeStamp = Long.toString(getTodayTime(0) / 1000);
    		String yesterdayTimeStamp = Long.toString(getTodayTime(-1) / 1000);
    		hql += " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') >= from_unixtime(cast(" + yesterdayTimeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')"
    			+  " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') < from_unixtime(cast(" + timeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')";
    	}
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FILTER_COLLECTION");
    }

	public static JavaRDD<TempNetTeleCallSkill> analysisAppFieldsRDD(DataFrame df) {
    	JavaRDD<Row> jRDD = df.select("ROWKEY", "USER_ID", "APP_TYPE", "APP_ID", 
    			"EVENT", "SUBTYPE", "LABEL", "CUSTOM_VAL", "VISIT_COUNT", "VISIT_TIME", "VISIT_DURATION").rdd().toJavaRDD();
    	JavaRDD<TempNetTeleCallSkill> pRDD = jRDD.map(new Function<Row, TempNetTeleCallSkill>() {

			private static final long serialVersionUID = 2887518971635512003L;

			public TempNetTeleCallSkill call(Row v1) throws Exception {
				// TODO Auto-generated method stub
				return SrcLogParse.analysisCallSkillLabel(v1.getString(0), v1.getString(1), v1.getString(2),
				         v1.getString(3), v1.getString(4), v1.getString(5), v1.getString(6),
				         v1.getString(7), v1.getString(8), v1.getString(9), v1.getString(10));
			}
    	});
    	return pRDD;
    }

    public DataFrame getNetTeleCallSkillDF2(HiveContext sqlContext) {
        String hql = "SELECT TMP_A.ROWKEY, "
        		+ "          TMP_A.APP_ID, "
        		+ "          TMP_A.USER_ID, "
        		+ "          TMP_A.USER_EVENT, "
        		+ "          TMP_A.FROM_PAGE, "
        		+ "          TMP_A.CALLSKILL_ID, "
        		+ "          TMP_A.CALLSKILL_TYPE, "
        		+ "          TMP_A.CREATED_USERID, "
                + "          TMP_A.VISIT_COUNT, "
                + "          TMP_A.VISIT_TIME, "
                + "          TMP_A.VISIT_DURATION, "
                + "          TMP_A.FIRST_LEVEL, "
                + "          TMP_A.SECOND_LEVEL, "
                + "          TMP_A.THIRD_LEVEL, "
                + "          TMP_A.FOURTH_LEVEL "
                + "    FROM  TMP_ANALYSISLABEL TMP_A";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_NETTELE_CALLSKILL");
    }
    
    public DataFrame getNetTeleCallSkillDF3(HiveContext sqlContext) {
        String hql = "SELECT DISTINCT TMP_UBT.ROWKEY, "
        		+ "        	 TMP_UBT.APP_ID, "
        		+ "          TMP_UBT.USER_ID, "
                + "          TMP_UBT.USER_EVENT, "
        		+ "          TMP_UBT.FROM_PAGE, "
        		+ "          TMP_UBT.CALLSKILL_ID, "
        		+ "          TMP_UBT.CALLSKILL_TYPE, "
        		+ "          TMP_UBT.CREATED_USERID, "
                + "          CAST(TMP_UBT.VISIT_COUNT AS STRING) as VISIT_COUNT, "
                + "          CAST(from_unixtime(cast(cast(TMP_UBT.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') AS STRING) as VISIT_TIME,"
                + "          CAST(TMP_UBT.VISIT_DURATION AS STRING) as VISIT_DURATION, "
                + "          TMP_UBT.FIRST_LEVEL, "
                + "          TMP_UBT.SECOND_LEVEL, "
                + "          TMP_UBT.THIRD_LEVEL, "
                + "          TMP_UBT.FOURTH_LEVEL "
                + "   FROM   TMP_NETTELE_CALLSKILL TMP_UBT";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "NETTELE_CALLSKILL_FOR_TELE");
    }

    public DataFrame getNetTeleCallSkillDF4(HiveContext sqlContext) {
        String hql = "SELECT  MIN(T4.ROWKEY) as ROWKEY, "
           		+ "        	  T4.APP_ID, "
        		+ "           T4.USER_ID, "
                + "           T4.USER_EVENT, "
        		+ "           T4.FROM_PAGE, "
        		+ "           T4.CALLSKILL_ID, "
        		+ "           T4.CALLSKILL_TYPE, "
        		+ "           T4.CREATED_USERID, "
                + "           T4.VISIT_COUNT, "
                + "           T4.VISIT_TIME, "
                + "           T4.VISIT_DURATION, "
                + "           T4.FIRST_LEVEL, "
                + "           T4.SECOND_LEVEL, "
                + "           T4.THIRD_LEVEL, "
                + "           T4.FOURTH_LEVEL "
        		+ "  FROM     NETTELE_CALLSKILL_FOR_TELE T4  "
        		+ "  GROUP BY T4.APP_ID, "
        		+ "           T4.USER_ID, "
                + "           T4.USER_EVENT, "
        		+ "           T4.FROM_PAGE, "
        		+ "           T4.CALLSKILL_ID, "
        		+ "           T4.CALLSKILL_TYPE, "
        		+ "           T4.CREATED_USERID, "
                + "           T4.VISIT_COUNT, "
                + "           T4.VISIT_TIME, "
                + "           T4.VISIT_DURATION, "
                + "           T4.FIRST_LEVEL, "
                + "           T4.SECOND_LEVEL, "
                + "           T4.THIRD_LEVEL, "
                + "           T4.FOURTH_LEVEL ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_NETTELE_CALLSKILL");
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return timeStamp;
	}

    public JavaRDD<FactNetTeleCallSkill> getJavaRDD(DataFrame df) {
    	JavaRDD<Row> jRDD = df.select("ROWKEY", "APP_ID", "USER_ID", "USER_EVENT", "FROM_PAGE", 
    			"CALLSKILL_ID", "CALLSKILL_TYPE", "CREATED_USERID", "VISIT_COUNT", "VISIT_TIME", "VISIT_DURATION", 
    			"FIRST_LEVEL", "SECOND_LEVEL", "THIRD_LEVEL", "FOURTH_LEVEL").rdd().toJavaRDD();
        JavaRDD<FactNetTeleCallSkill> pRDD = jRDD.map(new Function<Row, FactNetTeleCallSkill>() {

			private static final long serialVersionUID = 1233379052181842481L;

			public FactNetTeleCallSkill call(Row v1) throws Exception {
                return new FactNetTeleCallSkill(v1.getString(0), v1.getString(1), v1.getString(2), 
                        v1.getString(3), v1.getString(4), v1.getString(5), 
                        v1.getString(6), v1.getString(7), v1.getString(8), 
                        v1.getString(9), v1.getString(10), v1.getString(11), 
                        v1.getString(12), v1.getString(13), v1.getString(14));
            }
        });
        return pRDD;
    }
    
	public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<FactNetTeleCallSkill> rdd, String path) {
		if (TK_DataFormatConvertUtil.isExistsPath(path)) {
			String tmpPath = path + "-temp";
			Integer repartition_count = 200;
			try {
				repartition_count = Integer.valueOf(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_REPARTITION_COUNT));
			} catch (Exception e) {
				repartition_count = 200;
			}
			
			//Append new data to parquet[path]
			sqlContext.createDataFrame(rdd, FactNetTeleCallSkill.class).save(path, "parquet", SaveMode.Append);
			//Load parquet[path], save as parquet[tmpPath]
			TK_DataFormatConvertUtil.deletePath(tmpPath);
			sqlContext.load(path).repartition(repartition_count).saveAsParquetFile(tmpPath);
			//Delete parquet[path]
			TK_DataFormatConvertUtil.deletePath(path);
			//Rename parquet[tmpPath] as parquet[path]
			TK_DataFormatConvertUtil.renamePath(tmpPath, path);
		} else {
			sqlContext.createDataFrame(rdd, FactNetTeleCallSkill.class).saveAsParquetFile(path);
		}
	}
    
    public void saveAsParquet(DataFrame df, String path) {
		TK_DataFormatConvertUtil.deletePath(path);
		df.saveAsParquetFile(path);
	}
}
