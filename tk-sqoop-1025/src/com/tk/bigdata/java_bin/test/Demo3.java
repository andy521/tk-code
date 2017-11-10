package com.tk.bigdata.java_bin.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.yaml.snakeyaml.Yaml;

import com.tk.bigdata.java_bin.domain.DbBean;
import com.tk.bigdata.java_bin.domain.LogBean;
import com.tk.bigdata.java_bin.domain.SqoopBean;
import com.tk.bigdata.java_bin.utils.ConfigUtils;
import com.tk.bigdata.java_bin.utils.FileUtils;
import com.tk.bigdata.java_bin.utils.ProcessUtils;
import com.tk.bigdata.java_bin.utils.SqoopProcessUtils;
import com.tk.bigdata.java_bin.utils.TimeUtils;
import com.tk.bigdata.java_bin.utils.TimeoutDetection;

public class Demo3 {
	
	public static Map<String, SqoopBean> sqoopMap = new ConcurrentHashMap<>();
	
	public static BlockingQueue<SqoopBean> taskQueue = new LinkedBlockingQueue<>();
	public static BlockingQueue<SqoopBean> waitQueue = new LinkedBlockingQueue<>();
	public static Map<String, List<SqoopBean>> taskMap = new ConcurrentHashMap<>();
	
	public static ExecutorService executorService = null;
	
	public static String START_DAY = null;
	
	private final static HashMap<String, String> YAML_NAME = new HashMap<String, String>();
	static{
		YAML_NAME.put("tables_all_inc.yml", "正常增量倒数任务");
		YAML_NAME.put("tables_olbi_inc.yml", "BI库全量倒数任务");
		YAML_NAME.put("tables_tkorasol_memberupdate_inc.yml", "依赖顺序表增量倒数任务");
	}
	/** 获取yaml配置文件中文名 */
	private static String getYAMLName() {
		String name = new File(System.getProperty("config_path")).getName();
		String string = YAML_NAME.get(name);
		return string == null ? name : string;
	}
	
	public static void main(String[] args) throws Exception {
		System.setProperty("basic_path", "C:/Users/meisanfeng/Desktop/resource/");
		System.setProperty("config_path", "D:/Document/WeChat/WeChat Files/q907214568/Files/data.yml");
		if(args.length >= 4){
			System.setProperty("basic_path", args[0]);
			System.setProperty("config_path", args[1]);
			System.setProperty("thread_num", args[2]);
			System.setProperty("wechat_config", args[3]);
//		}else if(args.length >= 1){
//			System.setProperty("config_path", args[0]);
		}else{
			System.out.println("没有获取配置文件, 程序已终止!");
			System.exit(-1);
			return ;
		}
		
		START_DAY = TimeUtils.getTimeByDay();
		System.out.println("程序启动, 启动日期: " + START_DAY);
		FileUtils.validateFile(ProcessUtils.LOG_PATH + START_DAY + "/");
		FileUtils.validateFile(ProcessUtils.SYS_PATH + START_DAY + "/");
		FileUtils.validateFile(ProcessUtils.VALIDATE_PATH + START_DAY + "/");
		println(null);
		
		Yaml yaml = new Yaml();
		DbBean bean = yaml.loadAs(new FileInputStream(System.getProperty("config_path")), DbBean.class);
		List<SqoopBean> ls = ConfigUtils.loadingConfig(bean);
		
		int startTableCount = ls.size();
		System.out.println("加载表个数: " + ls.size());
		for (SqoopBean sqoopBean : ls) {
			taskQueue.put(sqoopBean);
		}
		
		//开启守护进行, 进行超时通知
		{
			try {
				TimeoutDetection.startTime = System.currentTimeMillis();
				TimeoutDetection.name = getYAMLName();
				new Thread(TimeoutDetection.td).start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		for (int i = 0; i < 3; i++) {
			int threadNum = 1;
			try {
				threadNum = Integer.parseInt(System.getProperty("thread_num").trim());
			} catch (Exception e) {
			}
			System.out.println("设置线程数量: " + threadNum);
			executorService = Executors.newFixedThreadPool(threadNum); 
			
			start(i);
			
			System.out.println("第["+i+"]次, waitQueue: "+waitQueue.size());
			if(waitQueue.size() == 0) {
				break;
			}
			while(true){
				SqoopBean poll = waitQueue.poll();
				if(poll == null) break;
				taskQueue.add(poll);
			}
		}
		
		{
			try {
				String result = null;
				if(taskQueue.size() == 0) {
					result = "成功";
				}else if(taskQueue.size() == startTableCount){
					result = "失败";
				}else{
					result = "部分成功";
				}
				TimeoutDetection.td.destory(result);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		//TODO 临时注释
//		if(waitQueue.size() == 0){
//			System.exit(0);
//		}else{
//			System.exit(-1);
//		}
		
	}

	
	
	public static void start(int i) throws Exception{
		while(true){
			SqoopBean sqoopBean = taskQueue.poll();
			if(sqoopBean == null) break;
			
			Demo3.sqoopMap.put(sqoopBean.getName(), sqoopBean);
			
			String commond = SqoopProcessUtils.renderCommond(sqoopBean);
			executorService.submit(new ProcessUtils(sqoopBean.getName(), commond, i == 2));
		}
		
		System.out.println("第"+i+"次任务加载完毕!");
		executorService.shutdown();
		int times = 1;
		
		try {              
			boolean loop = true;              
			do {    
				//等待所有任务完成                  
				loop = !executorService.awaitTermination(10, TimeUnit.SECONDS);  
				times ++;
				if(times % 10 == 0){
					System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + ": 运行中! " + loop);
				}
			} while(loop);      
			System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + ": 运行完成! " + loop);
		} catch (InterruptedException e) {              
			e.printStackTrace();          
		}
	}
	
	private static PrintWriter pw = null;
	public synchronized static void println(LogBean log) throws Exception{
		if(pw == null){
			pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(ProcessUtils.BASE_PATH + "count.log", true), "UTF-8"), true);
		}
		if(log == null){
			pw.println("\r\n===== "+TimeUtils.getTimeByNow()+" =====");
			pw.println(LogBean.getColumnName());
		}else{
			pw.println(log.toString());
		}
	}
	
}
