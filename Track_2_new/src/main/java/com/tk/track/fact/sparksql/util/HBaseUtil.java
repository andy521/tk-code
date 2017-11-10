package com.tk.track.fact.sparksql.util;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import com.tk.track.base.table.BaseTable;
import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;

import scala.Tuple2;

public class HBaseUtil {
	/*
	 * save rdd result to HBase
	 */
	public static void putRDD2Hbase(JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts, String tableName) {
		Configuration conf = HBaseUtil.InitTable(tableName);

		try {
			putRDD2Table(hbasePuts, tableName, conf);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/*
	 * Init Hbase Table
	 */
	private static Configuration InitTable(String tableName) {
		Configuration conf = HBaseConfiguration.create();
		conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, 
				TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
		conf.set(TableInputFormat.INPUT_TABLE, tableName);

		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (!admin.isTableAvailable(tableName)) {
				HTableDescriptor tableDesc = new HTableDescriptor(tableName);
				tableDesc.addFamily(new HColumnDescriptor(BaseTable.FAMILY));
				admin.createTable(tableDesc);
			}
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return conf;
	}

	private static void putRDD2Table(JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts, final String tableName,
			Configuration conf) throws IOException {
		try {
			HBaseAdmin.checkHBaseAvailable(conf);
			System.out.println("HBase is running!");
		} catch (MasterNotRunningException e) {
			System.out.println("HBase is not running!");
			System.exit(1);
		} catch (Exception ce) {
			ce.printStackTrace();
		}
		Job newAPIJobConfiguration1 = Job.getInstance(conf);
		newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
		newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
		

		hbasePuts.foreachPartition(new VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Put>>>() {

			public void call(Iterator<Tuple2<ImmutableBytesWritable, Put>> rows) throws Exception {
				// TODO Auto-generated method stub
				Configuration config = HBaseConfiguration.create();
				config.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, 
						TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
				HConnection conn = HConnectionManager.createConnection(config);
				HTableInterface htable = conn.getTable(tableName);
				htable.setAutoFlush(false);
				// htable.setWriteBufferSize(3 * 1024 * 1024);// 关键点2
				htable.setWriteBufferSize(htable.getWriteBufferSize());// 关键点2
				while (rows.hasNext()) {
					// save to hbase
					// rows.next()._2.setWriteToWAL(false);
					htable.put(rows.next()._2);
				}
				htable.flushCommits();

			}
		});
	}
	
	public static void put(String tableName,byte[] rowkey,byte[] family,byte[] qualifier,byte[] value){
		Configuration conf = HBaseConfiguration.create();
		conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
		try {
			HTable table=new HTable(conf,tableName);
			Put put=new Put(rowkey);
			put.add(family, qualifier, value);
			table.put(put);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}

