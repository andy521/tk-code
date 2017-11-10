package com.tk.track.util;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import com.tk.track.base.table.BaseTable;

import scala.Tuple2;

public class HBaseRDD {
	public static Long maxResultSize = (long) -1;
	public static Long maxMainSize = (long) 100;

	public static JavaPairRDD<ImmutableBytesWritable, Result> getRDD(JavaSparkContext jsc, String tableName,
			Long maxSize) {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
		conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
		conf.set(TableInputFormat.INPUT_TABLE, tableName);

		try {
			Scan scan = new Scan();
			// scan.setMaxResultSize(maxSize);
			scan.setCaching(10000);
			ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
			String ScanToString = Base64.encodeBytes(proto.toByteArray());
			conf.set(TableInputFormat.SCAN, ScanToString);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		JavaPairRDD<ImmutableBytesWritable, Result> myRDD = jsc.newAPIHadoopRDD(conf, TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class);

		return myRDD;
	}

	public static JavaPairRDD<ImmutableBytesWritable, Result> getRDD(JavaSparkContext jsc, String tableName) {
		return getRDD(jsc, tableName, maxResultSize);
	}

	public static void putRDD2Hbase(JavaSparkContext jsc, PairRDD2Save ppp, String tableName, Object o) {
		Configuration conf = initTable(tableName);

		try {
			putRDD2Table(jsc, ppp, tableName, o, conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static Configuration initTable(String tableName) {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
		conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
		conf.set(TableInputFormat.INPUT_TABLE, tableName);

		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (!admin.isTableAvailable(tableName)) {
				HTableDescriptor tableDesc = new HTableDescriptor(tableName);
				tableDesc.addFamily(new HColumnDescriptor(BaseTable.FAMILY));
				admin.createTable(tableDesc);
			}
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return conf;

	}

	private static void putRDD2Table(JavaSparkContext jsc, PairRDD2Save ppp, final String tableName, Object o,
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
		//
		Job newAPIJobConfiguration1 = Job.getInstance(conf);
		newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
		newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

		JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = ppp.pairRDD2Put(jsc, o);
		hbasePuts.foreachPartition(new VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Put>>>() {

			public void call(Iterator<Tuple2<ImmutableBytesWritable, Put>> rows) throws Exception {
				// TODO Auto-generated method stub
				HConnection conn = HConnectionManager.createConnection(HBaseConfiguration.create());
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

	public static void putRDD2HFile(JavaSparkContext jsc, PairRDD2Save ppp, String path, Object o) {
		try {
			putRDD(jsc, ppp, path, o);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void putRDD(JavaSparkContext jsc, PairRDD2Save ppp, String path, Object o) throws IOException {
		// try {
		// HBaseAdmin.checkHBaseAvailable(conf);
		// System.out.println("HBase is running!");
		// } catch (MasterNotRunningException e) {
		// System.out.println("HBase is not running!");
		// System.exit(1);
		// } catch (Exception ce) {
		// ce.printStackTrace();
		// }
		// //
		// Job newAPIJobConfiguration1 = Job.getInstance(conf);
		// newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,
		// tableName);
		// newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
		//
		// JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts =
		// ppp.pairRDD2Put(jsc, o);
		// hbasePuts.foreachPartition(new
		// VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Put>>>() {
		//
		// public void call(Iterator<Tuple2<ImmutableBytesWritable, Put>> rows)
		// throws Exception {
		// // TODO Auto-generated method stub
		// HConnection conn =
		// HConnectionManager.createConnection(HBaseConfiguration.create());
		// HTableInterface htable = conn.getTable(tableName);
		// htable.setAutoFlush(false);
		// // htable.setWriteBufferSize(3 * 1024 * 1024);// 关键点2
		// htable.setWriteBufferSize(htable.getWriteBufferSize());// 关键点2
		// while (rows.hasNext()) {
		// // save to hbase
		// rows.next()._2.setWriteToWAL(false);
		// htable.put(rows.next()._2);
		// }
		// htable.flushCommits();
		//
		// }
		// });

		// hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
		TK_DataFormatConvertUtil.deletePath(path);
		ppp.pairRDD2Hdfs(jsc, o, path);
		// if (ppp instanceof PairRDD2Writable) {
		// } else {
		// JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts =
		// ppp.pairRDD2Put(jsc);
		// hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
		// }
	}
}
