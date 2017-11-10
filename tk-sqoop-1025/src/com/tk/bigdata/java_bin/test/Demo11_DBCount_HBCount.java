package com.tk.bigdata.java_bin.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.yaml.snakeyaml.Yaml;

import com.tk.bigdata.java_bin.domain.DbBean;
import com.tk.bigdata.java_bin.domain.SqoopBean;
import com.tk.bigdata.java_bin.utils.ConfigUtils;
import com.tk.bigdata.java_bin.utils.FileUtils;
import com.tkonline.common.db.util.StringUtil;

/**
 * 统计db中的count和hbase中的count
 */
public class Demo11_DBCount_HBCount implements Runnable {
	
	public static String FILE_PATH = null;//Demo11_DBCount_HBCount.class.getClassLoader().getResource("table_where.t").getPath();
	
	public static ExecutorService executorService = null;
	
	static {
		executorService = Executors.newFixedThreadPool(10);
	}
	
	public static void main1(String[] args) throws Exception {/*
		if(new File("/etc/profile").exists()){
			if(args == null || args.length == 0){
				System.out.println("没有获取到配置文件!");
				return ;
			}
			FILE_PATH = args[0];
		}else{
			FILE_PATH = Demo11_DBCount_HBCount.class.getClassLoader().getResource("").getPath() + "table_where.t";
		}
		
		System.out.printf("【%1$s】程序开始...\r\n", getTimes());
		String day = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		String nowDay = day + " 00:00:00";
		
		String basePath = "./validate/" + day + "/";
		FileUtils.validateFile(basePath);
		
		Map<String, LinkInfo> linkMap = readFile();
		QueryRunner runner = new QueryRunner();
		
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream("result.log", true), "UTF-8"), true);
		pw.println("\r\n=================="+getTimes()+"==================");
		
		for (Entry<String, LinkInfo> linkInfo : linkMap.entrySet()) {
			try {
//				String key = linkInfo.getKey();
				LinkInfo value = linkInfo.getValue();
				
				Connection conn = null;
				String sql = "SELECT count(0) FROM $table_name";
				if(value.url.contains("oracle")){//是oracl链接
					conn = Demo7_JDBC.getConnectionOracle(value.url, value.username, value.password);
					sql += " WHERE $systime < to_date('$nowDay', 'yyyy-mm-dd hh24:mi:ss')";
				}else{
					conn = Demo7_JDBC.getConnectionMysql(value.url, value.username, value.password);
					sql += " WHERE $systime < STR_TO_DATE('$nowDay', '%Y-%m-%d %T')";
				}
				
				for (TableInfo tableInfo : value.tables) {
					try {
						//验证是否已经执行过, 执行过放弃跳过
						String filePath = basePath + value.schema + "_" + tableInfo.tableName;
						if(FileUtils.exists(filePath)) continue;
						
						long startTime = System.currentTimeMillis();
						
						String execSql = sql.replace("$table_name", ("".equals(value.schema) ? tableInfo.tableName : value.schema + "." + tableInfo.tableName));
						if(!"".equals(tableInfo.whereTime)){
							execSql = execSql
										 .replace("$nowDay", nowDay)
										 .replace("$systime", tableInfo.whereTime); 
						}else{
							execSql = execSql.substring(0, execSql.indexOf(" WHERE $systime"));
						}
						System.out.printf("【%3$s】【%1$s】执行SQL: %2$s \r\n", tableInfo.tableName, execSql, getTimes());
						Object dbCount = runner.query(conn, execSql, new ScalarHandler<>());
						System.out.printf("【%3$s】【%1$s】DB数据量: %2$s \r\n", tableInfo.tableName, dbCount, getTimes());
						//执行MR
						System.out.printf("【%2$s】【%1$s】执行ShellSQL: hbase org.apache.hadoop.hbase.mapreduce.RowCounter '%1$s' \r\n", tableInfo.hbaseName, getTimes());
						Process process = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", "hbase org.apache.hadoop.hbase.mapreduce.RowCounter '"+tableInfo.hbaseName+"'"});
						int status = process.waitFor();
						System.out.printf("【%3$s】【%1$s】MR脚本执行状态: %2$s \r\n", tableInfo.tableName, status, getTimes());
						String sysLog = readInputStream(process.getInputStream());
						System.out.printf("【%3$s】【%1$s】MR输出[getInputStream]: \r\n%2$s \r\n", tableInfo.tableName, sysLog, getTimes());
						
						String count = null;
						{
							//从正常输出中获取执行条数
							count = getRows(sysLog);
						}
						String errLog = readInputStream(process.getErrorStream());
						System.out.printf("【%3$s】【%1$s】MR输出[getErrorStream]: \r\n%2$s \r\n", tableInfo.tableName, errLog, getTimes());
						{
							//从错误输出中获取执行条数
							if(count == null)
								count = getRows(errLog);
						}
						System.out.printf("【%3$s】【%1$s】MR count: %2$s \r\n", tableInfo.tableName, count, getTimes());
						
						long endTime = System.currentTimeMillis();
						
						pw.println(String.format("【%1$s】【%2$s】【%3$s】【%4$s】【%5$s】【%6$s】【%7$s】【%8$s】【%9$s】", getTimes(), value.schema, tableInfo.tableName, tableInfo.hbaseName, dbCount, count, startTime, endTime, endTime - startTime));
						pw.flush();
						
						FileUtils.createFile(filePath);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		pw.flush();
		pw.close();
		
		System.out.printf("【%1$s】程序执行结束!\r\n", getTimes());
	*/}
	
	public static void main(String[] args) {
		if(new File("/etc/profile").exists()){
			if(args == null || args.length == 0){
				System.out.println("没有获取到配置文件!");
				return ;
			}
			FILE_PATH = args[0];
		}else{
			FILE_PATH = Demo11_DBCount_HBCount.class.getClassLoader().getResource("").getPath() + "table_where.t";
		}
		start(FILE_PATH);
	}
	
	public static void start(String configPath){
		try {

			Yaml yaml = new Yaml();
			DbBean bean = yaml.loadAs(new FileInputStream(configPath), DbBean.class);
			List<SqoopBean> ls = ConfigUtils.loadingConfig(bean);
			System.out.println("加载表个数: " + ls.size());

			pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream("result.log", true), "UTF-8"), true);
			pw.println("\r\n=================="+getTimes()+"==================");
			
			
			System.out.printf("【%1$s】程序开始...\r\n", getTimes());
			String day = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
			nowDay = day + " 00:00:00";
			
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(nowDay));
			calendar.add(Calendar.DAY_OF_YEAR, -1);
			
			yesDay = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
			
			basePath = "./validate/" + day + "/";
			FileUtils.validateFile(basePath);
			
			for (SqoopBean sqoopBean : ls) {
				executorService.submit(new Demo11_DBCount_HBCount(sqoopBean));
			}
			
			executorService.shutdown();
			
			try {
				boolean loop = true;
				do {
					//等待所有任务完成
					loop = !executorService.awaitTermination(10, TimeUnit.SECONDS);
					//阻塞，直到线程池里所有任务结束
					System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + ": 阻塞中! " + loop);
				} while(loop);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	

	private static String sStr = "Map input records=";
	private static String eStr = "Map output records=";
	private static String getRows(String str){
		if(str == null) return null;
		try {
			return str.substring(str.indexOf(sStr) + sStr.length(), str.indexOf(eStr)).trim();
		} catch (Exception e) {
			return null;
		}
	}
	
	private static String readInputStream(InputStream in) throws IOException{
		StringBuilder sb = new StringBuilder("");
		byte[] bs = new byte[1024 * 8];
		int len = 0;
		while((len = in.read(bs)) != -1){
			sb.append(new String(bs, 0, len));
		}
		if(in != null) in.close();
		return sb.toString();
	}
	
	private static String getTimes(){
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
	}
	
	public static Map<String, LinkInfo> readFile(){
		Map<String, LinkInfo> linkMap = new HashMap<>();
		try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(FILE_PATH), "UTF-8"));){
			String line = null;
			while((line = br.readLine()) != null){
				String[] ls = line.split("`");
				LinkInfo link = null;
				String key = ls[6] + ls[2];
				if(linkMap.containsKey(key)){
					link = linkMap.get(key);
				}else{
					link = new LinkInfo();
					linkMap.put(key, link);
					link.url = ls[6];
					link.username = ls[0];
					link.password = StringUtil.decrpt(ls[1]);
					link.schema = ls[2];
				}
				TableInfo tinfo = new TableInfo();
				tinfo.tableName = ls[3];
				tinfo.hbaseName = ls[4];
				tinfo.whereTime = ls[5];
				link.tables.add(tinfo);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return linkMap;
	}
	
	private static String basePath = null;
	private static String nowDay = null;
	private static String yesDay = null;
	private static PrintWriter pw = null;
//	pw.println("\r\n=================="+getTimes()+"==================");
	
	private static QueryRunner runner = new QueryRunner();
	private SqoopBean sqoopBean;
	public Demo11_DBCount_HBCount(SqoopBean sqoopBean) {
		this.sqoopBean = sqoopBean;
	}
	@Override
	public void run() {
		try {
			//验证是否已经执行过, 执行过放弃跳过
			String filePath = basePath + this.sqoopBean.getTableName();
			if(FileUtils.exists(filePath));
			
			long startTime = System.currentTimeMillis();
			//1.查出来现在的dbcount
			Object dbCount = null;
			{
				Connection conn = null;
				String sql = "SELECT count(0) FROM " + this.sqoopBean.getTableName();
				if(this.sqoopBean.getUrl().contains("oracle")){
					conn = Demo7_JDBC.getConnectionOracle(this.sqoopBean.getUrl(), this.sqoopBean.getUsername(), this.sqoopBean.getPassword());
					if(this.sqoopBean.getWhere() == null || "".equals(this.sqoopBean.getWhere())){
						
					}else{
						sql += " WHERE " + this.sqoopBean.getWhere().replace("$sysday(0)", "to_date('$sysday(0)', 'yyyy-mm-dd hh24:mi:ss')")
																   .replace("$sysday(-1)", "to_date('$sysday(-1)', 'yyyy-mm-dd hh24:mi:ss')");
					}
					
				}else{
					conn = Demo7_JDBC.getConnectionMysql(this.sqoopBean.getUrl(), this.sqoopBean.getUsername(), this.sqoopBean.getPassword());
					if(this.sqoopBean.getWhere() == null || "".equals(this.sqoopBean.getWhere())){
						
					}else{
						sql += " WHERE " + this.sqoopBean.getWhere().replace("$sysday(0)", "STR_TO_DATE('$sysday(0)', '%Y-%m-%d %T')")
								   								   .replace("$sysday(-1)", "STR_TO_DATE('$sysday(-1)', '%Y-%m-%d %T')");
					}
				}
				
				sql = sql.replace("$sysday(0)", nowDay).replace("$sysday(-1)", yesDay);
				
				System.out.printf("【%3$s】【%1$s】执行SQL: %2$s \r\n", this.sqoopBean.getTableName(), sql, getTimes());
				dbCount = runner.query(conn, sql, new ScalarHandler<>());
				System.out.printf("【%3$s】【%1$s】DB数据量: %2$s \r\n", this.sqoopBean.getTableName(), dbCount, getTimes());
			}
			//2.查出来hbasecount
			Object count = null;
			{
				System.out.printf("【%2$s】【%1$s】执行ShellSQL: hbase org.apache.hadoop.hbase.mapreduce.RowCounter '%1$s' \r\n", this.sqoopBean.getHbaseTableName(), getTimes());
				Process process = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", "hbase org.apache.hadoop.hbase.mapreduce.RowCounter '"+this.sqoopBean.getHbaseTableName()+"'"});
				int status = process.waitFor();
				System.out.printf("【%3$s】【%1$s】MR脚本执行状态: %2$s \r\n", this.sqoopBean.getTableName(), status, getTimes());

				String sysLog = readInputStream(process.getInputStream());
				System.out.printf("【%3$s】【%1$s】MR输出[getInputStream]: \r\n%2$s \r\n", this.sqoopBean.getTableName(), sysLog, getTimes());
				
				{
					//从正常输出中获取执行条数
					count = getRows(sysLog);
				}
				String errLog = readInputStream(process.getErrorStream());
				System.out.printf("【%3$s】【%1$s】MR输出[getErrorStream]: \r\n%2$s \r\n", this.sqoopBean.getTableName(), errLog, getTimes());
				{
					//从错误输出中获取执行条数
					if(count == null)
						count = getRows(errLog);
				}
				System.out.printf("【%3$s】【%1$s】MR count: %2$s \r\n", this.sqoopBean.getTableName(), count, getTimes());
				
				long endTime = System.currentTimeMillis();
				
				pw.println(String.format("【%1$s】【%2$s】【%3$s】【%4$s】【%5$s】【%6$s】【%7$s】【%8$s】【%9$s】", getTimes(), this.sqoopBean.getSchema(), this.sqoopBean.getTableName(), this.sqoopBean.getHbaseTableName(), dbCount, count, startTime, endTime, endTime - startTime));
				pw.flush();
				
				FileUtils.createFile(filePath);
			}
			
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
class LinkInfo {
	String url;
	String username;
	String password;
	String schema;
	List<TableInfo> tables = new ArrayList<>();
}

class TableInfo {
	String tableName;
	String hbaseName;
	String where;
	
	String whereTime;
}
