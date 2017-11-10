package com.tk.bigdata.java_bin.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.util.Arrays;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import com.tk.bigdata.java_bin.domain.LogBean;
import com.tk.bigdata.java_bin.domain.SqoopBean;
import com.tk.bigdata.java_bin.em.Status;
import com.tk.bigdata.java_bin.test.Demo3;
import com.tk.bigdata.java_bin.test.Demo7_JDBC;
import com.tk.bigdata.java_bin.wechat.WechatMessageUtil;
import com.tk.bigdata.java_bin.wechat.WechatTableBean;

public class ProcessUtils implements Runnable{
	
	public static String BASE_PATH = "";
	public static String LOG_PATH = "";
	public static String SYS_PATH = "";
	public static String VALIDATE_PATH = "";
	
	private static String START_EXEC = null;
	
	static {
		{//确定执行命令环境 liunx or windows
			if(new File("/etc/profile").exists()){
				START_EXEC = "/bin/sh ";
			}else{
				START_EXEC = "cmd /c ";
			}
			System.out.println("START_EXEC: " + START_EXEC);
		}
		
		{//日志路径,验证路径等初始化
			BASE_PATH = System.getProperty("basic_path");
			LOG_PATH = BASE_PATH + "log/";
			SYS_PATH = BASE_PATH + "sys/";
			VALIDATE_PATH = BASE_PATH + "validate/";
			
			FileUtils.validateFile(BASE_PATH);
			FileUtils.validateFile(LOG_PATH);
			FileUtils.validateFile(SYS_PATH);
			FileUtils.validateFile(VALIDATE_PATH);
		}
		
		System.out.println("程序启动成功, resource: "+LOG_PATH);
	}
	
	public synchronized static void exitProcess(String name, Status result) throws Exception {
		FileUtils.removeFile(VALIDATE_PATH + Demo3.START_DAY + "/" + name, result);
	}
	
	public synchronized static void writeLog(String name, String text){
		String path = LOG_PATH + Demo3.START_DAY + "/";
		FileUtils.validateFile(path);
		try(PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(FileUtils.createFile(path + name+".log"), true), "UTF-8"), true)){
			pw.print(text);
			pw.flush();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("记录日志出现异常");
		}
	}

	private static String getValidatePath(){
		String path = VALIDATE_PATH + Demo3.START_DAY + "/";
		FileUtils.validateFile(path);
		return path;
	}
	
	
	private String name;
	private String commond;
	private boolean send;
	public ProcessUtils(String name, String commond, boolean send) {
		this.name = name;
		this.commond = commond;
		this.send = send;
	}
	@Override
	public void run() {
		try {
			String path = getValidatePath() + this.name;
			if(FileUtils.exists(path)){
				System.out.println("脚本文件执行中...");
				return ;
			}
			
			try {
				FileUtils.createFile(path);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			
			{
				try {
					TimeoutDetection.startTask(name);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			{
				System.out.println("开始执行命令: " + commond);
				LogBean log = new LogBean();
				
				Status result = Status.success;
				
				//0.记录日志
				StringBuilder sb = new StringBuilder("");
				sb.append("----------------------------------\r\n- start time: ").append(TimeUtils.getTimeByTimes()).append("  -\r\n----------------------------------\r\n");
				sb.append("name: ").append(name).append("\r\n");
				sb.append("exec: ").append(commond).append("\r\n\r\n");

				SqoopBean sqoopBean = Demo3.sqoopMap.get(name);

				log.setName(sqoopBean.getName());
				log.setSchema(sqoopBean.getSchema());
				log.setTableName(sqoopBean.getTableName());
				
				long exportStartTime = 0L;
				long startTime = System.currentTimeMillis();
				log.setStartTime(startTime);
				
				try {
					//2.获取需要执行的数据长度
					Connection conn = null;
					try {
						if(sqoopBean.getUrl().contains("oracle")){
							conn = Demo7_JDBC.getConnectionOracle(sqoopBean.getUrl(), sqoopBean.getUsername(), sqoopBean.getPassword());
						}else{
							conn = Demo7_JDBC.getConnectionMysql(sqoopBean.getUrl(), sqoopBean.getUsername(), sqoopBean.getPassword());
						}
					} catch (Exception e) {
						System.out.println("获取数据库链接失败, 无法统计count数据, error: " + e.getMessage());
					}
					if(conn == null){
						result = Status.failed;
					}else{
						long queryStartTime = System.currentTimeMillis();
						log.setQueryStartTime(queryStartTime);
						
						QueryRunner runner = new QueryRunner();
						String countSql = SqoopProcessUtils.getCountSQL(sqoopBean);
						sb.append("count sql: ").append(countSql).append("\r\n");
						System.out.println("执行统计SQL:" + countSql);
						
						Object queryCount = null;
						if(sqoopBean.getUrl().contains("oracle")){
							BigDecimal count = runner.query(conn, countSql, new ScalarHandler<BigDecimal>("SQL_COUNT"));
							sb.append("count: ").append(count).append("\r\n");
							queryCount = count;
						}else{
							long count = runner.query(conn, countSql, new ScalarHandler<Long>("SQL_COUNT"));
							sb.append("count: ").append(count).append("\r\n");
							queryCount = count;
						}
						System.out.println("统计结果【"+sqoopBean.getTableName()+"】: " + queryCount);
						
						long queryEndTime = System.currentTimeMillis();
						log.setQueryEndTime(queryEndTime);
						log.setQueryExecTime(queryEndTime - queryStartTime);
						log.setQueryCount(queryCount + "");
						
						sb.append("query time: ").append(log.getQueryExecTime() / 1000.0).append("\r\n");
					}
					
					exportStartTime = System.currentTimeMillis();
					log.setExportStartTime(exportStartTime);
					
					{//判断是否需要执行before import
						if(!Utils.isEmpty(sqoopBean.getBeforeImport())){
							try {
								System.out.println("开始执行befor import");
								System.out.println("/bin/sh -c echo \""+sqoopBean.getBeforeImport()+"\"|hbase shell");
								Process processBeforeImport = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", "echo \""+sqoopBean.getBeforeImport()+"\"|hbase shell"});
								int waitFor = processBeforeImport.waitFor();
								System.out.println(sqoopBean.getName()+" befor_import状态: " + waitFor);
								if(waitFor != 0){//状态码异常, 获取异常信息
									{
										BufferedReader br = new BufferedReader(new InputStreamReader(processBeforeImport.getInputStream()));  
										String bfres = "INFO: " + sqoopBean.getName() + "\r\n";
								        String line;
								        while ((line = br.readLine()) != null) {  
								        	bfres += line + "\r\n";
								        }
								        br.close();
								        System.out.println(bfres);
									}
									{
										BufferedReader br = new BufferedReader(new InputStreamReader(processBeforeImport.getErrorStream()));  
										String bfres = "ERROR: " + sqoopBean.getName() + "\r\n";
										String line;
										while ((line = br.readLine()) != null) {  
											bfres += line + "\r\n";
										}
										br.close();
										System.out.println(bfres);
									}
								}
							} catch (Exception e) {
								System.out.printf("%1$s before import 执行出现异常, shell bin: %3$s, error: %2$s", sqoopBean.getName(), e.getMessage(), sqoopBean.getBeforeImport());
							}
						}
					}
					
					
					{
						//3.执行shell
						Process process = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", commond});
						
						//4.记录process状态
						int status = process.waitFor();
						System.out.println("【"+name+"】脚本状态: " + status);
						if(status != 0){
							result = Status.failed;
						}
						
						Thread.currentThread().setName(name+"-"+status);
						
						//5.获取shell的输出
						BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));  
				        String line;
				        while ((line = br.readLine()) != null) {  
				            sb.append(line).append("\r\n");
				        }
				        br.close();
				       
				        //5.获取shell的Error输出
				        sb.append("\r\nError info: \r\n");
				        BufferedReader errorBr = new BufferedReader(new InputStreamReader(process.getErrorStream()));  
				        String errorLine;
				        while ((errorLine = errorBr.readLine()) != null) {  
				        	sb.append(errorLine).append("\r\n");
				        }
				        errorBr.close();
						sb.append("\r\n");
						
						//5.获取执行条数和时间记录
						BufferedReader countBr = new BufferedReader(new InputStreamReader(new FileInputStream(ProcessUtils.SYS_PATH + Demo3.START_DAY + "/" + name + ".sys")));  
						//mapreduce.ImportJobBase: Transferred
						String countLine;
						boolean start = false;
						sb.append("\r\nGet count and time info\r\n");
						String valCountStr = "mapreduce.ImportJobBase: Retrieved ";
						int count = 0;
						while ((countLine = countBr.readLine()) != null) {
							if(countLine.contains("mapreduce.ImportJobBase: Transferred")) start = true;
							if(start){
								if(countLine.contains(valCountStr)){
									try {
										count = Integer.parseInt(countLine.substring(countLine.indexOf(valCountStr) + valCountStr.length()).replace("records.", "").trim());
										log.setExportCount(count + "");
									} catch (Exception e) {

									}
								}
								sb.append(countLine).append("\r\n");
							}
						}
						countBr.close();
						
						sb.append("sqoop count: ").append(count).append("\r\n\r\n");
					}
					
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("Shell 执行异常");
					sb.append("执行异常: " + Arrays.toString(e.getStackTrace()));
					result = Status.failed;
				}
				
				long endTime = System.currentTimeMillis();

				log.setExportEndTime(endTime);
				log.setExportExecTime(endTime - exportStartTime);
				sb.append("export time: ").append(log.getExportExecTime() / 1000.0).append("\r\n");
			    
				log.setEndTime(endTime);
				log.setExecTime(endTime - startTime);
				log.setResult(result);
				
				
				//6.退出执行任务
				try {
					Demo3.println(log);
					ProcessUtils.exitProcess(name, result);
					//如果任务状态为失败
					if(result == Status.failed){
						System.out.println("任务执行失败: " + sqoopBean.getName());
						Demo3.waitQueue.add(sqoopBean);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				sb.append("\r\n----------------------------------\r\n").append("- end time:   ").append(TimeUtils.getTimeByTimes()).append("  -\r\n----------------------------------\r\n");
				
				//7.记录到日志文件中
				ProcessUtils.writeLog(name, sb.toString());
				
				//如果数据出现偏差, 微信提醒
				try {
					System.out.println(log);
					
					if(log.getQueryCount() == null || !log.getQueryCount().equals(log.getExportCount())){
						if(result != Status.failed){
							WechatMessageUtil.send(new WechatTableBean(name, log.getQueryCount(), log.getExportCount(), log.getExecTime() + "ms", result));
						}
					}
					if(result == Status.failed && send){
						WechatMessageUtil.send(new WechatTableBean(name, log.getQueryCount(), log.getExportCount(), log.getExecTime() + "ms", result));
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				System.out.println("任务: " + name + "。执行完毕！");

			}

			{
				try {
					TimeoutDetection.finishTask(name);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
