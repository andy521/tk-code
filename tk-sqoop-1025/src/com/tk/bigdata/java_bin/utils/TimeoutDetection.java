package com.tk.bigdata.java_bin.utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.tk.bigdata.java_bin.wechat.WechatMessageUtil;

public class TimeoutDetection implements Runnable {
	
	@Override
	public void run() {
		while(flag){
			try {
				cycle();
				Thread.sleep(DEFAULT_SLEEP);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static TimeoutDetection td;
	public static String name = "";
	public static long startTime = 0;
	public static long finishTime = 0;
	
	static{
		td = new TimeoutDetection();
	}
	
	public synchronized static void startTask(String name){
		TimeoutDetectionBean t = td.new TimeoutDetectionBean();
		t.start = System.currentTimeMillis();
		tasks.put(name, t);
	}
	
	public synchronized static void finishTask(String name){
		if(tasks.containsKey(name)){//正常都会有的, 除非是有问题
			TimeoutDetectionBean t = tasks.remove(name);
			if(t.times.size() != 0){//时间超过了, 结束通知
				t.finish = System.currentTimeMillis();
				WechatMessageUtil.send(WechatMessageUtil.openids, String.format("\n表空间：%1$s \n开始时间：%2$s \n结束时间：%3$s \n执行时间：%4$s", name, formatter(t.start), formatter(t.finish), (t.finish - t.start)/(1000 * 60) + " min"));
			}
		}else{
			//TODO 有问题 发一下吧
			
		}
	}
	
	private synchronized static void cycle(){
		long time = System.currentTimeMillis();
		Iterator<String> it = tasks.keySet().iterator();
		while(it.hasNext()){
			String name = it.next();
			TimeoutDetectionBean t = tasks.get(name);
			long remainder = (time - t.start) / detectionTime;
			if(remainder > 0){//超过了
				/*if(t.times.contains(remainder)){//通知过了
					
				}else{//通知
					t.times.add(remainder);
					WechatMessageUtil.send(WechatMessageUtil.openids, String.format("\n表空间：%1$s \n开始时间：%2$s \n当前时间：%3$s \n超时时长：%4$s", name, formatter(t.start), formatter(time), ((time - t.start) / (1000 * 60))+" min"));
				}*/
				
				if(t.times.size() == 0){
					WechatMessageUtil.send(WechatMessageUtil.openids, String.format("\n表空间：%1$s \n开始时间：%2$s \n当前时间：%3$s \n超时时长：%4$s", name, formatter(t.start), formatter(time), ((time - t.start) / (1000 * 60))+" min"));
				}
				t.times.add(remainder);
			}
		}
	}
	
//	public static String openids = null;
	private static final int detectionTime = 1000 * 60 * 30;
	private static final int DEFAULT_SLEEP = 1000 * 30;
	
	private static Map<String, TimeoutDetectionBean> tasks = new ConcurrentHashMap<>();
	
	private boolean flag;
	public TimeoutDetection() {
		this.flag = true;
	}
	public void destory(String result){
		
		this.flag = false;
		
		TimeoutDetection.finishTime = System.currentTimeMillis();
		
		try {
//			WechatMessageUtil.send(WechatMessageUtil.openids, String.format("\n导数时间统计统计  \n执行文件：%1$s\n开始时间：%2$s \n结束时间：%3$s \n执行时间：%4$s", name, formatter(startTime), formatter(finishTime), ((finishTime - startTime) / (1000 * 60)) + " min"));
			WechatMessageUtil.send(WechatMessageUtil.openids, String.format("\n任务名称：%1$s\n开始时间：%2$s \n结束时间：%3$s \n执行时间：%4$s \n执行结果：%5$s", name, formatter(startTime), formatter(finishTime), ((finishTime - startTime) / (1000 * 60)) + " min", result));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private class TimeoutDetectionBean {
		long start;
		long finish;
		List<Long> times = new ArrayList<>();
		
	}
	
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	public static String formatter(long time){
		return sdf.format(new Date(time));
	}
}
