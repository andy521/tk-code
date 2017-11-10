package com.tk.es;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

public class Demo1 {
	
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set(ConfigurationOptions.ES_NODES, "10.130.159.36");
		conf.set(ConfigurationOptions.ES_PORT, "9200");
		conf.set(ConfigurationOptions.ES_RESOURCE, "customer/fulltext");
		conf.set(ConfigurationOptions.ES_INPUT_JSON, "true");

		conf.set(ConfigurationOptions.ES_NODES_CLIENT_ONLY, "true");
		conf.set(ConfigurationOptions.ES_NODES_DATA_ONLY, "false");
		conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, ""
				+ "");
		
		
//		conf.setBoolean("mapreduce.map.speculative", false);
//		conf.setBoolean("mapreduce.reduce.speculative", false);
//		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//		if(oArgs.length != 1){
//			System.exit(2);
//		}
		
//		Job job = Job.getInstance(conf, "5isTestJob");
		JobConf job = new JobConf(conf);
		job.setJarByClass(Demo1.class);
		
		
		job.setMapperClass(MyMapper.class);
		
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputFormat(EsOutputFormat.class);
		
		
//		FileInputFormat.addInputPath(job, new Path("hdfs://10.130.201.142:8020/user/tkonline/hj_test/input"));
		FileInputFormat.addInputPath(job, 
//				new Path("hdfs://10.130.159.11:8020/user/tkonline/taikangscore/data/user_portrait/user_info")
				new Path("hdfs://10.130.159.11:8020/user/tkonline/hj_test/input")
		);
		
		RunningJob runJob = JobClient.runJob(job);
		
		runJob.waitForCompletion();
		
//		JobConf jobConf = new JobConf();
//		jobConf.setSpeculativeExecution(false);
//		jobConf.set("es.nodes", "localhost:9200");
//		jobConf.set("es.resource", "taikangscore/user_portrait");
//		jobConf.setOutputFormat(EsOutputFormat.class);
//		jobConf.setMapOutputValueClass(Text.class);
//		jobConf.setMapperClass(MyMapper.class);
//		
//		JobClient.runJob(jobConf);
	}
	
	
}

@SuppressWarnings({ "unchecked", "rawtypes" })
class MyMapper extends MapReduceBase implements Mapper{
	
	public void map(Object key, Object value, OutputCollector output,
			Reporter reporter) throws IOException {
//		MapWritable doc = new MapWritable();
		System.out.println(value);
		output.collect(NullWritable.get(), value);
	}
	
}

