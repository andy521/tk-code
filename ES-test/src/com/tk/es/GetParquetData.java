package com.tk.es;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetReader.Builder;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

public class GetParquetData {
	
	public static void main(String[] args) throws Exception {
		
		if(args.length == 0){
			System.out.println("no file path");
			return ;
		}
		
//		ESThread.init();
		
		for (String input : args) {
			if(input == null || "".equals(input) || !input.startsWith("hdfs://")){
				System.out.println("error: " + input);
			}else{
				parquetReader(input);
			}
		}
		
		CommonQueue.finish();
		
//		String input = "hdfs://10.130.201.142:8020/user/tkonline/hj_test/input";
//		String input = "hdfs://10.130.201.142:8020/user/tkonline/hj_test/parquet";
		
		
//		parquetWriter("", input);
	}


	private static void parquetReader(String input) throws IOException {
		GroupReadSupport readSupport = new GroupReadSupport();
		Builder<Group> reader = ParquetReader.builder(readSupport, new Path(input));
		ParquetReader<Group> build = reader.build();
		Group line = null;
		
//		int count = 0;
		while((line = build.read()) != null){
			try {
				Map<String, String> data = new HashMap<>();
				
				GroupType type = line.getType();
				List<Type> fields = type.getFields();
				for (Type t : fields) {
					String name = t.getName();
					String val = null;
					try {
						val = line.getString(name, 0);
						if("".equals(val)) val = null;
					} catch (Exception e) {
						val = null;
					}
					
					if(val != null){
						data.put(name, val);
					}
				}
				
				CommonQueue.put(data);
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			
//			if(count++ > 10) break;
		}
		
	}
	
	/*private static void parquetWriter(String output, String input) throws Exception, IOException {
		MessageType schema = MessageTypeParser.parseMessageType(input);
		GroupFactory factory = new SimpleGroupFactory(schema);
		
		Configuration configuration = new Configuration(); 
		GroupWriteSupport writeSupport = new GroupWriteSupport();
		GroupWriteSupport.setSchema(schema, configuration);
		ParquetWriter<Group> writer = new ParquetWriter<Group>(new Path(output), configuration, writeSupport);
		
	}*/
	
}
