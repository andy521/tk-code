package com.tk.es;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class MapReduceFileToES {
	
	public static void main(String[] args) {
		try {
			if(args.length == 0 || !new File(args[0]).exists()){
				System.out.println("no file path");
				return;
			}
			
			start(args[0]);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void start(String path) throws Exception {
		Properties properties = new Properties();
		properties.load(new FileInputStream(new File(path)));
		String jobName = (String) properties.remove("job.name");
		String hdfsPaths = (String) properties.remove("hdfs.paths");

		Configuration conf = new Configuration();
		conf.set(ConfigurationOptions.ES_NODES, "10.130.159.36");
		conf.set(ConfigurationOptions.ES_PORT, "9200");
		conf.set(ConfigurationOptions.ES_RESOURCE, "tk/user_portrait");
		conf.set(ConfigurationOptions.ES_INPUT_JSON, "true");
		conf.set(ConfigurationOptions.ES_MAPPING_ID, "user_id");
		conf.set(ConfigurationOptions.ES_WRITE_OPERATION,
				ConfigurationOptions.ES_OPERATION_UPSERT);
		
		for (Object key : properties.keySet()) {
			String skey = (String) key;
			conf.set(skey, properties.getProperty(skey));
		} 

		Job job = Job.getInstance(conf, MapReduceFileToES.class.getSimpleName());
		job.setJobName(jobName);
		job.setJarByClass(MapReduceFileToES.class);
		job.setSpeculativeExecution(false);

		//map输入格式
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPaths(job, hdfsPaths);
		
		//设置map
		job.setMapperClass(MapperES.class);
		
		//设置map输出的key-value格式
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(ReduceEs.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//设置任务输出格式
		job.setOutputFormatClass(EsOutputFormat.class);
		
		
		
		// hdfs:///user/tkonline/taikangscore/data/user_portrait/user_info/part-r-00199-234245e6-efaf-4c79-aef0-0e10dc598725.gz.parquet

//		job.setNumReduceTasks(0);
		job.waitForCompletion(true);
	}


	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static class MapperES extends
			Mapper<Writable, Text, Text, Text> {
		@Override
		protected void map(Writable key, Text value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			
			try {
				JSONObject json = JSONObject.parseObject(value.toString());
				
				String userId = json.getString("uid");
				
				if(userId == null){
					userId = "NULL" + (UUID.randomUUID().toString().replace("-", ""));
				}
				
				context.write(new Text(userId), value);
			} catch (Exception e) {
				e.printStackTrace();
			}

		} 
	}
	
	@SuppressWarnings({"rawtypes", "unchecked"})
	public static class ReduceEs extends Reducer{
		@Override
		protected void reduce(Object key, Iterable values, Context content)
				throws IOException, InterruptedException {
			
			Map map = new HashMap();
			
			Iterator it = values.iterator();
			while(it.hasNext()){
				Text next = (Text)it.next();
				JSONObject json = JSONObject.parseObject(next.toString());
				for(Entry<String, Object> entry: json.entrySet()){
					map.put(entry.getKey(), entry.getValue());
				}
			}
			
			if(!map.containsKey("user_id")){
				map.put("user_id", key + "");
			}
			
			
			content.write(NullWritable.get(), JSON.toJSONString(map));
		}
	}

}
