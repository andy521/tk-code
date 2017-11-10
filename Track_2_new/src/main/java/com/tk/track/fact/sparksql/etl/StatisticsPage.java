package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.base.table.BaseTable;
import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;
import com.tk.track.fact.sparksql.desttable.FactStatisticsPage;

import scala.Tuple2;

public class StatisticsPage implements Serializable {

	private static final long serialVersionUID = -4039072670177268548L;
	String sca = TK_DataFormatConvertUtil.getSchema();
	
	public DataFrame getStatisticsPageDF(HiveContext sqlContext) {
		String hql = "SELECT A.ROWKEY, A.APP_TYPE, A.APP_ID, A.PAGE,"
				+ "          A.REFER, A.CURRENTURL, A.TIME, A.DURATION,A.FROM_ID"
				+ " FROM     Temp_SrcUserEvent A ";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FACT_STATISTICS_PAGE");
	}
	
	
	public JavaRDD<FactStatisticsPage> getJavaRDD(DataFrame df) {
		
		RDD<Row> rdd = df.select("ROWKEY", "APP_TYPE", "APP_ID", "PAGE",
				                 "REFER", "CURRENTURL", "TIME", "DURATION","FROM_ID").rdd();
		JavaRDD<Row> jRDD = rdd.toJavaRDD();
		
		JavaRDD<FactStatisticsPage> pRDD = jRDD.map(new Function<Row, FactStatisticsPage>() {

			private static final long serialVersionUID = 8544751955279802509L;

			public FactStatisticsPage call(Row v1) throws Exception {
				// TODO Auto-generated method stub
//				String rowKey = IDUtil.getUUID().toString();
				return new FactStatisticsPage(v1.getString(0), v1.getString(1), v1.getString(2), 
						v1.getString(3), v1.getString(4), v1.getString(5), 
						v1.getString(6), v1.getString(7),v1.getString(8));
			}
		});

		return pRDD;
	}
	
	public JavaPairRDD<ImmutableBytesWritable, Put> getPut(JavaRDD<FactStatisticsPage> rdd) {
		JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts =
				rdd.mapToPair(new PairFunction<FactStatisticsPage, ImmutableBytesWritable, Put>() {

					private static final long serialVersionUID = 8648017036249107277L;

					public Tuple2<ImmutableBytesWritable, Put> call(FactStatisticsPage t) throws Exception {
						FactStatisticsPage p = (FactStatisticsPage)t;
						Put put = new Put(BaseTable.getBytes(p.getROWKEY()));
						
						put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("ROWKEY"),
								BaseTable.getBytes(p.getROWKEY()));
						put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("APP_TYPE"),
								BaseTable.getBytes(p.getAPP_TYPE()));
						put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("APP_ID"),
								BaseTable.getBytes(p.getAPP_ID()));
						put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("PAGE"),
								BaseTable.getBytes(p.getPAGE()));
						put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("REFER"),
								BaseTable.getBytes(p.getREFER()));
						put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("CURRENTURL"),
								BaseTable.getBytes(p.getCURRENTURL()));
						put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("TIME"),
								BaseTable.getBytes(p.getTIME()));
						put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("DURATION"),
								BaseTable.getBytes(p.getDURATION()));
						
						return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
					}

				});
		return hbasePuts;
	}
	
	public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<FactStatisticsPage> rdd, String path) {
		if (TK_DataFormatConvertUtil.isExistsPath(path)) {
			String tmpPath = path + "-temp";
			Integer repartition_count = 200;
			try {
				repartition_count = Integer.valueOf(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_REPARTITION_COUNT));
			} catch (Exception e) {
				repartition_count = 200;
			}
			
			// Append new data to parquet[path]
			sqlContext.createDataFrame(rdd, FactStatisticsPage.class).save(path, "parquet", SaveMode.Append);
			// Load parquet[path], save as parquet[tmpPath]
			TK_DataFormatConvertUtil.deletePath(tmpPath);
			sqlContext.load(path).repartition(repartition_count).saveAsParquetFile(tmpPath);
			// Delete parquet[path]
			TK_DataFormatConvertUtil.deletePath(path);
			// Rename parquet[tmpPath] as parquet[path]
			TK_DataFormatConvertUtil.renamePath(tmpPath, path);
		} else {
			sqlContext.createDataFrame(rdd, FactStatisticsPage.class).saveAsParquetFile(path);
		}
	}
	
}
