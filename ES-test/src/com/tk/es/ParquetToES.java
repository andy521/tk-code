package com.tk.es;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.api.DelegatingReadSupport;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;

public class ParquetToES {

	public static void main(String[] args) {
		try {
			if (args.length == 0 || !new File(args[0]).exists()) {
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

		createIndex(properties);

		Configuration conf = new Configuration();
		conf.set(ConfigurationOptions.ES_NODES, "10.130.159.36");
		conf.set(ConfigurationOptions.ES_PORT, "9200");
		conf.set(ConfigurationOptions.ES_RESOURCE, "tk/user_portrait");
		conf.set(ConfigurationOptions.ES_INPUT_JSON, "true");
		conf.set(ConfigurationOptions.ES_MAPPING_ID, "user_id");
		conf.set(ConfigurationOptions.ES_WRITE_OPERATION,
				ConfigurationOptions.ES_OPERATION_UPSERT);

		// conf.set(ConfigurationOptions.ES_BATCH_WRITE_RETRY_COUNT, "50");
		// conf.set(ConfigurationOptions.ES_BATCH_WRITE_RETRY_WAIT, "100s");
		// conf.set(ConfigurationOptions.ES_BATCH_SIZE_ENTRIES_DEFAULT, "6000");
		//
		// conf.set("mapreduce.job.running.map.limit", "50");

		for (Object key : properties.keySet()) {
			String skey = (String) key;
			conf.set(skey, properties.getProperty(skey));
		}

		Job job = Job.getInstance(conf, ParquetToES.class.getSimpleName());
		job.setJobName(jobName);
		job.setJarByClass(ParquetToES.class);
		job.setSpeculativeExecution(false);

		// map输入格式
		job.setInputFormatClass(ParquetInputFormat.class);
		ParquetInputFormat.setReadSupportClass(job, MyReadSupport.class);
		ParquetInputFormat.addInputPaths(job, hdfsPaths);

		// 设置map
		job.setMapperClass(MapperES.class);
		// 设置map输出的key-value格式
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(ReduceEs.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 设置任务输出格式
		job.setOutputFormatClass(EsOutputFormat.class);
		job.waitForCompletion(true);
	}

	private static void createIndex(Properties properties) throws Exception {
		String node = properties.getProperty(ConfigurationOptions.ES_NODES)
				.split(",")[0];
		String port = properties.getProperty(ConfigurationOptions.ES_PORT);
		String indexName = properties.getProperty(
				ConfigurationOptions.ES_RESOURCE).split("/")[0];

		String url = String.format("http://%s:%s/%s", node, port, indexName);

		if (!JsoupConnection.head(url)) {
			// 创建索引
			JsoupConnection.put(url, null);
		}

		Map<String, Object> params = new HashMap<>();
		params.put("number_of_replicas", 0);
		params.put("refresh_interval", "-1");
		Map<String, Object> body = new HashMap<>();
		body.put("index", params);
		// setting
		JsoupConnection.put(url + "/_settings", JSON.toJSONString(body));

	}

	public static final class MyReadSupport extends
			DelegatingReadSupport<Group> {
		public MyReadSupport() {
			super(new GroupReadSupport());
		}

		@Override
		public org.apache.parquet.hadoop.api.ReadSupport.ReadContext init(
				InitContext context) {
			return super.init(context);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static class MapperES extends Mapper<Writable, Group, Text, Text> {
		@Override
		protected void map(Writable key, Group value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {

			Map data = new HashMap();

			GroupType type = value.getType();
			List<Type> fields = type.getFields();
			for (Type t : fields) {
				String name = t.getName();
				String val = null;
				try {
					val = value.getString(name, 0);
					if ("".equals(val))
						val = null;
				} catch (Exception e) {
					val = null;
				}

				data.put(name, val);
			}

			String userId = (String) data.get("user_id");

			if (userId == null) {
				userId = "NULL"
						+ (UUID.randomUUID().toString().replace("-", ""));
			}

			context.write(
					new Text(userId),
					new Text(JSON.toJSONString(data,
							SerializerFeature.WriteMapNullValue)));
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static class ReduceEs extends Reducer {

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
		}

		@Override
		protected void reduce(Object key, Iterable values, Context content)
				throws IOException, InterruptedException {

			Map map = new HashMap();

			Iterator it = values.iterator();
			while (it.hasNext()) {
				Text next = (Text) it.next();
				JSONObject json = JSONObject.parseObject(next.toString());
				for (Entry<String, Object> entry : json.entrySet()) {
					String k = entry.getKey();
					String oldValue = (String) map.get(k);

					if (oldValue == null || "".equals(oldValue)) {
						map.put(k, entry.getValue());
					}
				}
			}
			content.write(NullWritable.get(),
					JSON.toJSONString(map, SerializerFeature.WriteMapNullValue));
		}
	}
}
