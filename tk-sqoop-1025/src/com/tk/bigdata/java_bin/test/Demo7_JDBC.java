package com.tk.bigdata.java_bin.test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;

import oracle.jdbc.driver.OracleDriver;

public class Demo7_JDBC {
	
	public static void main(String[] args) throws Exception {
		if (args == null){
			System.out.println("没有获取到配置信息");
			return ;
		}
		
		QueryRunner runner = new QueryRunner();
		Connection conn = null;
		if(args.length >= 3){
			conn = getConnectionOracle(args[0], args[1], args[2]);
		}else if(args.length >= 1){
			InputStream in = new FileInputStream(args[0]);
			Properties prop = new Properties();
			prop.load(in);
			in.close();
			
			conn = getConnectionOracle(prop.getProperty("url"), prop.getProperty("user"), prop.getProperty("pwd"));

			String tableName = prop.getProperty("name");
			List<Map<String, Object>> tableInfo = runner.query(conn, "DESC " + tableName, new MapListHandler());
			System.out.println(tableName + ": " + tableInfo);
		}else{
			System.out.println("没有获取到配置信息");
			return ;
		}
	
		
		conn.close();
	}
	
	public synchronized static Connection getConnectionOracle(String url, String username, String password) throws Exception{
		Class.forName("oracle.jdbc.driver.OracleDriver");
		DriverManager.registerDriver(new OracleDriver());
//		password = StringUtil.decrpt(password);
		return DriverManager.getConnection(url, username, password);
	}
	
	public static Connection getConnectionMysql(String url, String username, String password) throws Exception{
		Class.forName("com.mysql.jdbc.Driver");
		return DriverManager.getConnection(url, username, password);
	}
	
}
