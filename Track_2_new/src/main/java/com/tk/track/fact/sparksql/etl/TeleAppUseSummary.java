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
import com.tk.track.fact.sparksql.desttable.TempTeleAppUseSummary;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

public class TeleAppUseSummary implements Serializable {
	private static final long serialVersionUID = 5241589537279973504L;

	public DataFrame getTeleAppUseSummaryDF(HiveContext sqlContext) {
		getTeleAppUseSummaryStartup(sqlContext);
		getTeleAppUseSummaryShutdown(sqlContext);
		getTeleAppUseSummaryTemp(sqlContext);
		return getTeleAppUseSummaryResult(sqlContext);
	}
	
	private DataFrame getTeleAppUseSummaryStartup(HiveContext sqlContext) {
		String hql = "SELECT " + "USER_ID," + "TERMINAL_ID," + "SUBTYPE,"
				+ "cast(TMP_TELEAPPUSESUMMARY.TIME as bigint) AS VISIT_TIME "
				+ "FROM TMP_TELEAPPUSESUMMARY WHERE EVENT='appUse' AND SUBTYPE = '1'";
		DataFrame df = DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_TELE_APPUSESUMMARY_STARTUP");
		System.out.println("================getTeleAppUseSummaryStartup================" + df.count());
		df.show(10);
		return df;

	}

	private DataFrame getTeleAppUseSummaryShutdown(HiveContext sqlContext) {
		String hql = "SELECT " + "USER_ID," + "TERMINAL_ID," + "SUBTYPE,"
				+ "cast(TMP_TELEAPPUSESUMMARY.TIME as bigint) AS VISIT_TIME "
				+ "FROM TMP_TELEAPPUSESUMMARY WHERE EVENT='appUse' AND SUBTYPE = '2'";
		DataFrame df = DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_TELE_APPUSESUMMARY_SHUTDOWN");
		System.out.println("================getTeleAppUseSummaryShutdown================" + df.count());
		df.show(10);
		return df;
	}
/*
 * 拥有结束时间对应多个开始时间的情况（需要去掉）
 * 
 */
	private DataFrame getTeleAppUseSummaryTemp(HiveContext sqlContext) {
//		String hql = "SELECT RET.USER_ID,RET.TERMINAL_ID,from_unixtime(cast(cast(RET.STARTTIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') AS STARTTIME,"
//				+ "		MIN(from_unixtime(cast(cast(RET.ENDTIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss')) AS ENDTIME"
//				+ "		FROM("
//				+ "			   SELECT U.USER_ID AS USER_ID,U.TERMINAL_ID AS TERMINAL_ID,U.VISIT_TIME AS STARTTIME,D.VISIT_TIME AS ENDTIME"
//				+ "				FROM TMP_TELE_APPUSESUMMARY_STARTUP AS U "
//				+ "					LEFT JOIN TMP_TELE_APPUSESUMMARY_SHUTDOWN AS D "
//				+ "					ON( U.USER_ID=D.USER_ID" 
//				+ "					AND U.TERMINAL_ID=D.TERMINAL_ID"
//				+ "					AND U.VISIT_TIME<D.VISIT_TIME) WHERE U.VISIT_TIME>0 AND D.VISIT_TIME>0"
//				+ "			) AS RET" 
//				+ "		GROUP BY RET.USER_ID,RET.TERMINAL_ID,RET.STARTTIME";
		String hql = "SELECT TEMP.USER_ID,TEMP.TERMINAL_ID,TEMP.ENDTIME,MAX(TEMP.STARTTIME) AS STARTTIME "
					+ " FROM("
					+ "		SELECT U.USER_ID AS USER_ID,U.TERMINAL_ID AS TERMINAL_ID,U.VISIT_TIME AS STARTTIME,D.VISIT_TIME AS ENDTIME"
					+ " 	FROM TMP_TELE_APPUSESUMMARY_SHUTDOWN AS D"
					+ "		LEFT JOIN TMP_TELE_APPUSESUMMARY_STARTUP AS U "
					+ "		ON( U.USER_ID=D.USER_ID "
					+ "		AND U.TERMINAL_ID=D.TERMINAL_ID"
					+ " 	AND U.VISIT_TIME<D.VISIT_TIME) "
					+ "		WHERE U.VISIT_TIME>0 AND D.VISIT_TIME>0"
					+ ") AS TEMP "
					+ " GROUP BY TEMP.USER_ID,TEMP.TERMINAL_ID,TEMP.ENDTIME";
		DataFrame df = DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_TELE_APPUSESUMMARY_TEMP");
		System.out.println("================getTeleSystemUseConditionResult================" + df.count());
		df.show(10);
		return df;
	}
	
	private DataFrame getTeleAppUseSummaryResult(HiveContext sqlContext) {
		String hql = "SELECT TEMP.USER_ID,"
				+ "			TEMP.TERMINAL_ID,"
				+ "			from_unixtime(cast(cast(TEMP.STARTTIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') AS STARTTIME,"
				+ "			MIN(from_unixtime(cast(cast(TEMP.ENDTIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss')) AS ENDTIME,"
				+ "			CAST(MIN(TEMP.ENDTIME-TEMP.STARTTIME) AS STRING) DURATION"
				+ "	  FROM TMP_TELE_APPUSESUMMARY_TEMP AS TEMP"
				+ "	  GROUP BY TEMP.USER_ID,TEMP.TERMINAL_ID,TEMP.STARTTIME";
		DataFrame df = DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_TELE_APPUSESUMMARY_RET");
		System.out.println("================getTeleSystemUseConditionResult================" + df.count());
		df.show(10);
		return df;
	}
	

	public JavaRDD<TempTeleAppUseSummary> getJavaRDD(DataFrame df) {
		JavaRDD<Row> jRDD = df.select("USER_ID", "TERMINAL_ID", "STARTTIME", "ENDTIME", "DURATION").rdd().toJavaRDD();
		JavaRDD<TempTeleAppUseSummary> pRDD = jRDD.map(new Function<Row, TempTeleAppUseSummary>() {
			private static final long serialVersionUID = 2887518971635512003L;

			public TempTeleAppUseSummary call(Row v1) throws Exception {
				return new TempTeleAppUseSummary(v1.getString(0), v1.getString(1), v1.getString(2), v1.getString(3), v1.getString(4));
			}
		});
		return pRDD;
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

	public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<TempTeleAppUseSummary> rdd, String path) {
		if (TK_DataFormatConvertUtil.isExistsPath(path)) {
			String tmpPath = path + "-temp";
			Integer repartition_count = 200;
			try {
				repartition_count = Integer
						.valueOf(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_REPARTITION_COUNT));
			} catch (Exception e) {
				repartition_count = 200;
			}

			// Append new data to parquet[path]
			sqlContext.createDataFrame(rdd, TempTeleAppUseSummary.class).save(path, "parquet", SaveMode.Append);
			// Load parquet[path], save as parquet[tmpPath]
			TK_DataFormatConvertUtil.deletePath(tmpPath);
			sqlContext.load(path).repartition(repartition_count).saveAsParquetFile(tmpPath);
			// Delete parquet[path]
			TK_DataFormatConvertUtil.deletePath(path);
			// Rename parquet[tmpPath] as parquet[path]
			TK_DataFormatConvertUtil.renamePath(tmpPath, path);
		} else {
			sqlContext.createDataFrame(rdd, TempTeleAppUseSummary.class).saveAsParquetFile(path);
		}
	}

}
