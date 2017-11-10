package com.tk.kafka;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

/**
 * save data to hbase
 * @author itw_meisf
 *
 */
public class Demo01_spark1 implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public Configuration configuration; 
	public Connection connection;
	public TableName name;
	public Table table;
	
	public void init(String tableName) throws Exception{
		configuration = HBaseConfiguration.create();
		//configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.zookeeper.quorum", "10.130.159.11:2181,10.130.159.12:2181,10.130.159.13:2181");
		configuration.set("mapreduce.job.queuename", "exporter");
		
		connection = ConnectionFactory.createConnection(configuration);
		
		name = TableName.valueOf(tableName);
		
		createTable(false);
	}
	
	@SuppressWarnings({ "deprecation", "resource" })
	public void createTable(boolean del) throws IOException{
		HBaseAdmin hbAdmin = new HBaseAdmin(configuration);
		if(hbAdmin.tableExists(name)){
			if(del){
				hbAdmin.disableTable(name);
				hbAdmin.deleteTable(name);
			}else{
				return ;
			}
		}
		HTableDescriptor descriptor = new HTableDescriptor(name);
		descriptor.addFamily(new HColumnDescriptor("info"));
		hbAdmin.createTable(descriptor);
	}
	
	public synchronized void getTable() throws IOException{
		if(table == null){
			table = connection.getTable(name);
		}
	}
	
	public void insertData(String rowkey, Map<String, Object> data) throws Exception{
		getTable();
		List<Put> list = new ArrayList<>();
		for(Entry<String, Object> entry: data.entrySet()){
			Put put = new Put(rowkey.getBytes());
			String key = entry.getKey();
			Object val = entry.getValue();
			
			put.add(new KeyValue(rowkey.getBytes(), "info".getBytes(), key.getBytes(), (val == null ? null : val.toString().getBytes())));
			list.add(put);
		}
		
		table.put(list);
	}
	
	public void insertData(User user) throws Exception{
		Map<String, Object> data = new HashMap<String, Object>();
		Method[] methods = User.class.getMethods();
		for (Method method : methods) {
			String methodName = method.getName();
			if(methodName.startsWith("get") && !methodName.startsWith("getClass")){
				Object val = method.invoke(user);
				data.put(exchange(methodName), val);
			}
		}
		insertData(UUID.randomUUID().toString(), data);
	}
	
	public String exchange(String methodName){
		if(methodName.startsWith("get")){
			methodName = methodName.substring(3);
		}
		String fName = "";
		for (int i = 0; i < methodName.length(); i++) {
			char c = methodName.charAt(i);
			if(c < 97){
				fName += "_";
			}
			fName += c;
		}
		return (fName = fName.toUpperCase()).startsWith("_") ? fName.substring(1) : fName;
	}
	
	public List<Map<String, Object>> insertDatas() throws Exception{
		List<Map<String, Object>> list = new ArrayList<>();
		Random rand = new Random();
		for (int i = 0; i < 10; i++) {
			Map<String, Object> data = new HashMap<String, Object>();
			User user = new User(UUID.randomUUID().toString(), rand.nextInt(100) + "");
			
			Method[] methods = User.class.getMethods();
			for (Method method : methods) {
				String methodName = method.getName();
				if(methodName.startsWith("get") && !methodName.startsWith("getClass")){
					Object val = method.invoke(user);
					data.put(exchange(methodName), val);
				}
			}
			list.add(data);
		}
		return list;
	}
	
	public static void main(String[] args) throws Exception {

	}
	
}
