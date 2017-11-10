package com.tk.es;

import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.EsSpark;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

public class SparkParquetToEs implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public String startTime = "";
	public String index = "";
	
	public static void main(String[] args) {
		try {
			if(args.length == 0 || !new File(args[0]).exists()){
				System.out.println("no file path");
				return ;
			}
			
			SparkParquetToEs es = new SparkParquetToEs();
			es.startTime = LocalDate.now().toString();
			
			es.parquetToES(args[0]);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("serial")
	private void parquetToES(String proPath) throws Exception {
		Properties prop = new Properties();
		prop.load(new FileInputStream(proPath));
		
		String jobName = (String) prop.remove("job.name");
		String hdfsPaths = (String) prop.remove("hdfs.paths");
		
		createIndex(prop);
		
		SparkConf conf = new SparkConf();
		conf.setAppName(jobName);
		conf.set("es.nodes", "10.130.159.36");
		conf.set("es.port", "9200");
		conf.set("es.input.json", "true");
//		conf.set("es.write.operation", "upsert");//使用这种方式的时候必要要自定义ES的主键ID
		conf.set("es.query", "?pipline=mypid");
		
		for(Object key : prop.keySet()){
			String sKey = (String) key;
			conf.set(sKey, prop.getProperty(sKey));
		}
		
		SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
		SQLContext sqlContext = sparkSession.sqlContext();
		Dataset<Row> parquet = sqlContext.read().parquet(hdfsPaths.split(","));
		
		String[] columns = parquet.columns();
		
		Dataset<String> res = parquet.map(new MapFunction<Row, String>() {
			@Override
			public String call(Row row) throws Exception {
				Map<String, Object> map = new HashMap<>();
				for (String column : columns) {
					Object val = row.getAs(column);
					map.put(column, val == null ? null : val + "");
				}
				map.put("_sys_timestamp", startTime);
				return JSON.toJSONString(map, SerializerFeature.WriteMapNullValue);
			}
		}, Encoders.STRING());
		
		EsSpark.saveJsonToEs(res.rdd(), index);
		
	}

	private void createIndex(Properties prop) throws Exception {
		String node = prop.getProperty("es.nodes").split(",")[0];
		String port = prop.getProperty("es.port");
		String[] resources = prop.getProperty("es.resource").split("/");
		String indexName = resources[0] + "_" + this.startTime.replace("-", "");
		
		this.index = indexName + "/" + resources[1];
		
		String url = String.format("http://%s:%s/%s", node, port, indexName);
		
		if(JsoupConnection.head(url)){
			return ;
		}
		
		//{"mappings": {"%s": {"dynamic_date_formats": [%s]}}, "settings": {"index": {"num_of_replicas": 0, "refresh_interval": "10s"}}}
		String body = String.format("{\"mappings\": {\"%s\": {\"dynamic_date_formats\": [%s]}}, \"settings\": {\"index\": {\"num_of_replicas\": 0, \"refresh_interval\": \"10s\"}}}", resources[1], prop.remove("date.type"));
		JsoupConnection.put(url, body);
	}
	
}
