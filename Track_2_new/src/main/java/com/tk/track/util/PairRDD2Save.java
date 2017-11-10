package com.tk.track.util;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public interface PairRDD2Save {
	public JavaPairRDD<ImmutableBytesWritable, Put> pairRDD2Put(JavaSparkContext jsc, Object o);
	public void pairRDD2Hdfs(JavaSparkContext jsc, Object o, String path) ;
}
