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
import com.tk.track.fact.sparksql.desttable.TeleUseCondition;
import com.tk.track.fact.sparksql.desttable.TempTeleUseCondition;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.IDUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

public class TeleSystemUseCondition implements Serializable{
	private static final long serialVersionUID = 5241589537279973504L;
	 public DataFrame getTeleSystemUseConditionDF(HiveContext sqlContext, String appids) {
	    	
	    	if(appids==null || appids.equals("")){
	    		return null;
	    	}
	    	
	    	DataFrame teleUseConditionDF=filterTeleUseIDDF(sqlContext,appids);
	    	
	    	 JavaRDD<TempTeleUseCondition> labelRDD= analysisAppFieldsRDD(teleUseConditionDF);
	        
	         sqlContext.createDataFrame(labelRDD, TempTeleUseCondition.class).registerTempTable("TMP_TELE_CONDITION");
	    	
	         return getTeleSystemUseConditionResult(sqlContext);
	 }
	 
	 private DataFrame filterTeleUseIDDF(HiveContext sqlContext,String appids) {
		 String hql = "SELECT TUC1.USER_ID,TUC1.APP_TYPE,TUC1.APP_ID,"
				    +"TUC1.EVENT as MAIN_MENU,"
				    +"TUC1.SUBTYPE as SUB_MENU,"				 				 
	                + "from_unixtime(cast(cast(TUC1.TIME as bigint) / 1000 as bigint),'yyyy-MM-dd') as VISIT_TIME,"
				    + "TUC1.LABEL"
	                + " FROM  Temp_teleUseCondition TUC1 " 
	                + " WHERE LOWER(TUC1.APP_TYPE)='system' and LOWER(TUC1.APP_ID) in ("+ appids.toLowerCase() +")"
	                + " AND TUC1.event <> 'page.load' AND TUC1.event <> 'page.unload' "
	                + " AND (TUC1.currenturl like '%10.129.118.207%' or TUC1.currenturl like '%10.129.118.123%') ";
	    	
	        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_TELE_USE_CONDITION");
	    }
	 
	 public static JavaRDD<TempTeleUseCondition> analysisAppFieldsRDD(DataFrame df) {
	    	JavaRDD<Row> jRDD = df.select( "USER_ID", "VISIT_TIME", "APP_TYPE", "APP_ID", 
	    			"MAIN_MENU", "SUB_MENU","LABEL").rdd().toJavaRDD();
	    	JavaRDD<TempTeleUseCondition> pRDD = jRDD.map(new Function<Row, TempTeleUseCondition>() {

				private static final long serialVersionUID = 2887518971635512003L;

				public TempTeleUseCondition call(Row v1) throws Exception {
					// TODO Auto-generated method stub
					return SrcLogParse.analysisTeleUseLabel(v1.getString(0), v1.getString(1), v1.getString(2),
					         v1.getString(3), v1.getString(4), v1.getString(5), v1.getString(6));
				}
	    	});
	    	return pRDD;
	    }

	 private DataFrame getTeleSystemUseConditionResult(HiveContext sqlContext) {
	        String hql = "SELECT  CUT.USER_ID,"
	        		+ "           CUT.VISIT_TIME,"
	        		+ "           CUT.MAIN_MENU,"
	        		+ "           CUT.SUB_MENU,"
	        		+ "           CUT.FUNCTION_DESC,"
	        		+ "           string(COUNT(*)) AS CLICK_COUNT"
	                + "     FROM  TMP_TELE_CONDITION CUT"
	                + "     GROUP BY CUT.USER_ID," 
	                + "           CUT.VISIT_TIME,"
	                + "           CUT.APP_TYPE," 
	                + "           CUT.APP_ID,"
	                + "           CUT.MAIN_MENU," 
	                + "           CUT.SUB_MENU,"
	                + "           CUT.FUNCTION_DESC";	                           
	        return DataFrameUtil.getDataFrame(sqlContext, hql, "TELE_USE_CONDITION");
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
	 public JavaRDD<TeleUseCondition> getJavaRDD(DataFrame df) {
	    	JavaRDD<Row> jRDD = df.select("USER_ID", "VISIT_TIME", "MAIN_MENU", "SUB_MENU", 
	    			"FUNCTION_DESC", "CLICK_COUNT").rdd().toJavaRDD();
	        JavaRDD<TeleUseCondition> pRDD = jRDD.map(new Function<Row, TeleUseCondition>() {

				private static final long serialVersionUID = 1233379052181842481L;

				public TeleUseCondition call(Row v1) throws Exception {
					String rowKey ="";
					String EVENT = v1.getString(2);
	                rowKey = IDUtil.getUUID().toString();
	                return new TeleUseCondition(rowKey,v1.getString(0), v1.getString(1), EVENT, 
	                        v1.getString(3), v1.getString(4), v1.getString(5));
	            }
	        });
	        return pRDD;
	    }
	 
	 
	 public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<TeleUseCondition> rdd, String path) {
			if (TK_DataFormatConvertUtil.isExistsPath(path)) {
				String tmpPath = path + "-temp";
				Integer repartition_count = 200;
				try {
					repartition_count = Integer.valueOf(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_REPARTITION_COUNT));
				} catch (Exception e) {
					repartition_count = 200;
				}
				
				//Append new data to parquet[path]
				sqlContext.createDataFrame(rdd, TeleUseCondition.class).save(path, "parquet", SaveMode.Append);
				//Load parquet[path], save as parquet[tmpPath]
				TK_DataFormatConvertUtil.deletePath(tmpPath);
				sqlContext.load(path).repartition(repartition_count).saveAsParquetFile(tmpPath);
				//Delete parquet[path]
				TK_DataFormatConvertUtil.deletePath(path);
				//Rename parquet[tmpPath] as parquet[path]
				TK_DataFormatConvertUtil.renamePath(tmpPath, path);
			} else {
				sqlContext.createDataFrame(rdd, TeleUseCondition.class).saveAsParquetFile(path);
			}
		}
}
