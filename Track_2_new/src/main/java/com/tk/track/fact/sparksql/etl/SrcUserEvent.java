package com.tk.track.fact.sparksql.etl;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.base.table.BaseTable;
import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.desttable.FactSrcUserEvent;
import com.tk.track.util.TK_DataFormatConvertUtil;

import scala.Tuple2;

public class SrcUserEvent implements Serializable {

    private static final long serialVersionUID = -1661550661674716313L;

    public JavaRDD<FactSrcUserEvent> getJavaRDD(JavaRDD<String> jRDD) {

        JavaRDD<FactSrcUserEvent> pRDD = jRDD.rdd().toJavaRDD().filter(new Function<String, Boolean>() {

            private static final long serialVersionUID = 7536776026814652654L;

            public Boolean call(String v1) throws Exception {
                if (v1 == null || v1.equals("") || (!v1.contains("dummy"))) {
                    return false;
                }
                return true;
            }
        }).flatMap(new FlatMapFunction<String, FactSrcUserEvent>() {

            private static final long serialVersionUID = -5729384531216635493L;

            public List<FactSrcUserEvent> call(String v1) throws Exception {
                return SrcLogParse.analysisContext(v1);
            }

        });

        return pRDD;
    }

    public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<FactSrcUserEvent> rdd, String path) {
    	if (TK_DataFormatConvertUtil.isExistsPath(path)){
    		String tmpPath = path + "-temp";
			Integer repartition_count = 200;
			try {
				repartition_count = Integer.valueOf(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_REPARTITION_COUNT));
			} catch (Exception e) {
				repartition_count = 200;
			}
			
			// Append new data to parquet[path]
	        sqlContext.createDataFrame(rdd, FactSrcUserEvent.class).save(path, "parquet", SaveMode.Append);
			// Load parquet[path], save as parquet[tmpPath]
			TK_DataFormatConvertUtil.deletePath(tmpPath);
			sqlContext.load(path).repartition(repartition_count).saveAsParquetFile(tmpPath);
			// Delete parquet[path]
			TK_DataFormatConvertUtil.deletePath(path);
			// Rename parquet[tmpPath] as parquet[path]
			TK_DataFormatConvertUtil.renamePath(tmpPath, path);
			
    	} else {
            sqlContext.createDataFrame(rdd, FactSrcUserEvent.class).saveAsParquetFile(path);
    	}
    }

    public void pairRDD2ParquetDaily(HiveContext sqlContext, JavaRDD<FactSrcUserEvent> rdd, String path) {
        TK_DataFormatConvertUtil.deletePath(path);
        sqlContext.createDataFrame(rdd, FactSrcUserEvent.class).saveAsParquetFile(path);
    }

    public String getDailyPath(String path) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        String yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
        return (path + "-" + yesterday);
    }

    public JavaPairRDD<ImmutableBytesWritable, Put> getPut(JavaRDD<FactSrcUserEvent> rdd) {
        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = rdd
                .mapToPair(new PairFunction<FactSrcUserEvent, ImmutableBytesWritable, Put>() {

                    private static final long serialVersionUID = 8467469016074628773L;

                    public Tuple2<ImmutableBytesWritable, Put> call(FactSrcUserEvent t) throws Exception {
                        FactSrcUserEvent p = (FactSrcUserEvent) t;
                        Put put = new Put(BaseTable.getBytes(p.getROWKEY()));

                        put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("ROWKEY"),
                                BaseTable.getBytes(p.getROWKEY()));
                        put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("IP"),
                                BaseTable.getBytes(p.getIP()));
                        put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("TIME"),
                                BaseTable.getBytes(p.getTIME()));
                        put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("APP_TYPE"),
                                BaseTable.getBytes(p.getAPP_TYPE()));
                        put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("APP_ID"),
                                BaseTable.getBytes(p.getAPP_ID()));
                        put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("USER_ID"),
                                BaseTable.getBytes(p.getUSER_ID()));
                        put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("PAGE"),
                                BaseTable.getBytes(p.getPAGE()));
                        put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("EVENT"),
                                BaseTable.getBytes(p.getEVENT()));
                        put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("SUBTYPE"),
                                BaseTable.getBytes(p.getSUBTYPE()));
                        put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("LABEL"),
                                BaseTable.getBytes(p.getLABEL()));
                        put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("CUSTOM_VAL"),
                                BaseTable.getBytes(p.getCUSTOM_VAL()));
                        put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("REFER"),
                                BaseTable.getBytes(p.getREFER()));
                        put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("CURRENTURL"),
                                BaseTable.getBytes(p.getCURRENTURL()));
                        put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("BROWSER"),
                                BaseTable.getBytes(p.getBROWSER()));
                        put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("SYSINFO"),
                                BaseTable.getBytes(p.getSYSINFO()));
                        put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("TERMINAL_ID"),
                                BaseTable.getBytes(p.getTERMINAL_ID()));
                        put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("DURATION"),
                                BaseTable.getBytes(p.getDURATION()));
                        put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("CLIENTTIME"),
                                BaseTable.getBytes(p.getCLIENTTIME()));
                        return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
                    }

                });
        return hbasePuts;
    }

    public void putRDD2HFile(JavaPairRDD<ImmutableBytesWritable, Put> rdd, String filename) throws IOException {
        Configuration conf = new Configuration();
        Job newAPIJobConfiguration1 = Job.getInstance(conf);
        newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, filename);
        newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

        rdd.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
    }

}
