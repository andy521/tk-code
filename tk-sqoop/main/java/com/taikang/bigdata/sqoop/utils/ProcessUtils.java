package com.taikang.bigdata.sqoop.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class ProcessUtils{
	
	public static int mapSize = 5;
	public static BlockingQueue<String[]> waitQueue;
	
	public static String BASE_PATH = "";
	public static String LOG_PATH = "";
	public static String SYS_PATH = "";
	public static String VALIDATE_PATH = "";
	
	private static String START_EXEC = null;
	private static Map<String, Thread> processMap;
	
	static {
		{//确定执行命令环境 liunx or windows
			if(new File("/etc/profile").exists()){
				START_EXEC = "/bin/sh ";
			}else{
				START_EXEC = "cmd /c ";
			}
			System.out.println("START_EXEC: " + START_EXEC);
		}
		
		{//初始化
			processMap = new ConcurrentHashMap<>();
			waitQueue = new LinkedBlockingQueue<>();
		}
		
		{//日志路径,验证路径等初始化
			//BASE_PATH = ExecUtils.class.getClassLoader().getResource("").getPath();
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
	
	
	/**
	 * 异步执行一个脚本
	 * @param name	唯一的执行名称
	 * @param commond	脚本执行命令
	 */
	public static synchronized void execProcess(String name, String commond) throws Exception{
		String path = getValidatePath() + name;
		if(FileUtils.exists(path)){
			System.out.println("脚本文件执行中...");
			return ;
		}
		if(processMap.size() >= mapSize){
			waitQueue.add(new String[]{name, commond});
			System.out.println("当前线程池数量: " + processMap.size() + "。任务等待中【"+name+"】");
			return ;
		}
		
		
		FileUtils.createFile(path);
		
		Thread thread = new Thread(){
			@Override
			public void run() {
				System.out.println("开始执行命令: " + commond);
				
				String result = "success";
				//1.记录日志
				StringBuilder sb = new StringBuilder("");
				sb.append("----------------------------------\r\n- start time: ").append(TimeUtils.getTimeByTimes()).append("  -\r\n----------------------------------\r\n");
				sb.append("name: ").append(name).append("\r\n");
				sb.append("exec: ").append(commond).append("\r\n\r\n");
				try {
					//2.添加验证文件
					FileUtils.createFile(VALIDATE_PATH + TimeUtils.getTimeByDay() + "/" + name);
					
					//3.执行shell
					Process process = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", commond});
					
					//4.记录process状态
					int status = process.waitFor();
					System.out.println("脚本状态: " + status);
					
					currentThread().setName(name+"-"+status);
					
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
					BufferedReader countBr = new BufferedReader(new InputStreamReader(new FileInputStream(ProcessUtils.SYS_PATH + TimeUtils.getTimeByDay() + "/" + name + ".sys")));  
					//mapreduce.ImportJobBase: Transferred
					String countLine;
					boolean start = false;
					sb.append("\r\nGet count and time info\r\n");
					while ((countLine = countBr.readLine()) != null) {
						if(countLine.contains("mapreduce.ImportJobBase: Transferred")) start = true;
						if(start){
							sb.append(countLine).append("\r\n");
						}
					}
					countBr.close();
			        sb.append("\r\n");
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("Shell 执行异常");
					sb.append("执行异常: " + Arrays.toString(e.getStackTrace()));
					result = "failed";
				}
				
				//6.退出执行任务
				try {
					ProcessUtils.exitProcess(name, result);
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				sb.append("\r\n----------------------------------\r\n").append("- end time:   ").append(TimeUtils.getTimeByTimes()).append("  -\r\n----------------------------------");
				
				//7.记录到日志文件中
				ProcessUtils.writeLog(name, sb.toString());
				
				System.out.println("任务: " + name + "。执行完毕！");
				System.out.println("当前任务数量: " + processMap.size());
			}
		};
		
		processMap.put(name, thread);
		
		thread.start();
	}

	public synchronized static void exitProcess(String name, String result) throws Exception {
		//移除验证文件
		FileUtils.removeFile(VALIDATE_PATH + TimeUtils.getTimeByDay() + "/" + name, result);
		//杀死执行脚本的线程
		Thread t = processMap.remove(name);
		if(t != null){
			t.interrupt();
		}
		
		while(processMap.size() < mapSize){
			String[] commonds = waitQueue.poll();
			if(commonds == null){
				break;
			}
			execProcess(commonds[0], commonds[1]);
		}
	}
	
	public static void writeLog(String name, String text){
		String path = LOG_PATH + TimeUtils.getTimeByDay()+"/";
		FileUtils.validateFile(path);
		try(PrintWriter pw = new PrintWriter(FileUtils.createFile(path + name+".log"))){
			pw.print(text);
			pw.flush();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("记录日志出现异常");
		}
	}

	private static String getValidatePath(){
		String path = VALIDATE_PATH+TimeUtils.getTimeByDay()+"/";
		FileUtils.validateFile(path);
		return path;
	}
	
	
	
}
