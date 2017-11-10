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
import com.tk.track.fact.sparksql.desttable.FactUserEvent;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

import scala.Tuple2;

public class UserEvent implements Serializable {

	private static final long serialVersionUID = -6500723757808043708L;
	String sca = TK_DataFormatConvertUtil.getSchema();
	
	public DataFrame getUserEventDF(HiveContext sqlContext) {
		String hql = "SELECT A.ROWKEY,"
				+ "          A.USER_ID, "
				+ "          A.APP_TYPE, "
				+ "          A.APP_ID, "
				+ "          A.PAGE,"
				+ "          A.EVENT,"
				+ "          A.SUBTYPE,"
				+ "          A.LABEL,"
				+ "          A.SYSINFO,"
				+ "          A.TERMINAL_ID,"
				+ "          A.TIME,"
				+ "			 A.FROM_ID"
				+ "   FROM   Temp_SrcUserEvent A ";
		
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FACT_USER_EVENT");
	}
	
	public JavaRDD<FactUserEvent> getJavaRDD(DataFrame df) {
		
		RDD<Row> rdd = df.select("ROWKEY", "USER_ID", "APP_TYPE", "APP_ID", 
				         "PAGE", "EVENT", "SUBTYPE", "LABEL", "SYSINFO", "TERMINAL_ID", "TIME","FROM_ID").rdd();
		JavaRDD<Row> jRDD = rdd.toJavaRDD();
		
		JavaRDD<FactUserEvent> pRDD = jRDD.map(new Function<Row, FactUserEvent>() {

			private static final long serialVersionUID = 2846237164612201812L;

			public FactUserEvent call(Row v1) throws Exception {
				// TODO Auto-generated method stub
//				String rowKey = IDUtil.getUUID().toString();
				return new FactUserEvent(v1.getString(0), v1.getString(1), v1.getString(2),
						v1.getString(3), v1.getString(4), v1.getString(5), v1.getString(6),
						v1.getString(7), v1.getString(8), v1.getString(9), v1.getString(10),v1.getString(11));
			}
		});

		return pRDD;
	}
	
	public JavaPairRDD<ImmutableBytesWritable, Put> getPut(JavaRDD<FactUserEvent> rdd) {
		JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts =
				rdd.mapToPair(new PairFunction<FactUserEvent, ImmutableBytesWritable, Put>() {

					private static final long serialVersionUID = 8648017036249107277L;

					public Tuple2<ImmutableBytesWritable, Put> call(FactUserEvent t) throws Exception {
						FactUserEvent p = (FactUserEvent)t;
						Put put = new Put(BaseTable.getBytes(p.getROWKEY()));
						
						put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("ROWKEY"),
								BaseTable.getBytes(p.getROWKEY()));
						if (null != p.getUSER_ID() && !p.getUSER_ID().equals(""))
						put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("USER_ID"),
								BaseTable.getBytes(p.getUSER_ID()));
						put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("APP_TYPE"),
								BaseTable.getBytes(p.getAPP_TYPE()));
						put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("APP_ID"),
								BaseTable.getBytes(p.getAPP_ID()));
						put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("PAGE"),
								BaseTable.getBytes(p.getPAGE()));
						if (null != p.getEVENT())
						put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("EVENT"),
								BaseTable.getBytes(p.getEVENT()));
						put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("SUBTYPE"),
								BaseTable.getBytes(p.getSUBTYPE()));
						put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("LABEL"),
								BaseTable.getBytes(p.getLABEL()));
						put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("SYSINFO"),
								BaseTable.getBytes(p.getSYSINFO()));
						put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("TERMINAL_ID"),
								BaseTable.getBytes(p.getTERMINAL_ID()));
						put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("TIME"),
								BaseTable.getBytes(p.getTIME()));
						
						return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
					}

				});
		return hbasePuts;
	}
	
	public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<FactUserEvent> rdd, String path) {
		if(TK_DataFormatConvertUtil.isExistsPath(path)){
			String tmpPath = path + "-temp";
			Integer repartition_count = 200;
			try {
				repartition_count = Integer.valueOf(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_REPARTITION_COUNT));
			} catch (Exception e) {
				repartition_count = 200;
			}
			
			// Append new data to parquet[path]
			sqlContext.createDataFrame(rdd, FactUserEvent.class).save(path, "parquet", SaveMode.Append);
			// Load parquet[path], save as parquet[tmpPath]
			TK_DataFormatConvertUtil.deletePath(tmpPath);
			sqlContext.load(path).repartition(repartition_count).saveAsParquetFile(tmpPath);
			// Delete parquet[path]
			TK_DataFormatConvertUtil.deletePath(path);
			// Rename parquet[tmpPath] as parquet[path]
			TK_DataFormatConvertUtil.renamePath(tmpPath, path);
		}else{
			sqlContext.createDataFrame(rdd, FactUserEvent.class).saveAsParquetFile(path);
		}
	}
}
