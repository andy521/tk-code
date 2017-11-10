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
import com.tk.track.fact.sparksql.desttable.TempTeleShareSummary;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

public class TeleShareSummary implements Serializable {
	private static final long serialVersionUID = 5241589537279973504L;

	public DataFrame getTeleShareSummaryDF(HiveContext sqlContext) {
		return getTeleAppUseSummaryAll(sqlContext);
	}

	private DataFrame getTeleAppUseSummaryAll(HiveContext sqlContext) {
		String hql = "SELECT A.USER_ID,A.SUBTYPE AS SHARETYPE,A.EVENT AS SHARECONTENT ,from_unixtime(cast(cast(A.TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') AS SHARETIME"
				+ " FROM TMP_TELESHARESUMMARY AS A WHERE A.EVENT IN ('planShare','seedShare') AND A.SUBTYPE IN ('1','2','3')";
		DataFrame df = DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_TELE_SHARESUMMARY_ALL");
		System.out.println("================getTeleShareSummaryDF================" + df.count());
		return df;
	}

	public JavaRDD<TempTeleShareSummary> getJavaRDD(DataFrame df) {
		JavaRDD<Row> jRDD = df.select("USER_ID", "SHARETYPE", "SHARETIME", "SHARECONTENT").rdd().toJavaRDD();
		JavaRDD<TempTeleShareSummary> pRDD = jRDD.map(new Function<Row, TempTeleShareSummary>() {
			private static final long serialVersionUID = 2887518971635512003L;

			public TempTeleShareSummary call(Row v1) throws Exception {
				return new TempTeleShareSummary(v1.getString(0), v1.getString(1), v1.getString(2), v1.getString(3));
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

	public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<TempTeleShareSummary> rdd, String path) {
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
			sqlContext.createDataFrame(rdd, TempTeleShareSummary.class).save(path, "parquet", SaveMode.Append);
			// Load parquet[path], save as parquet[tmpPath]
			TK_DataFormatConvertUtil.deletePath(tmpPath);
			sqlContext.load(path).repartition(repartition_count).saveAsParquetFile(tmpPath);
			// Delete parquet[path]
			TK_DataFormatConvertUtil.deletePath(path);
			// Rename parquet[tmpPath] as parquet[path]
			TK_DataFormatConvertUtil.renamePath(tmpPath, path);
		} else {
			sqlContext.createDataFrame(rdd, TempTeleShareSummary.class).saveAsParquetFile(path);
		}
	}

}
