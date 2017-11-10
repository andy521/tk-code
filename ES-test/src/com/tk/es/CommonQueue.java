package com.tk.es;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class CommonQueue {

	private static final int TIME_SLEEP = 5;
	
	private static BlockingQueue<Map<String, String>> queue = null;
	private static boolean finish = false;
	public static void finish(){
		finish = true;
	}
	static {
		queue = new LinkedBlockingQueue<>();
	}
	
	public static int count = 0;
	public static void put(Map<String, String> data){
		if(count ++ % 10000 == 0)
			System.out.println(String.format("userid: %s, count: %s", data.get("user_id"), count ++));
		queue.add(data);
	}
	
	public static Map<String, String> get(){
		Map<String, String> data = null;
		while(true){
			try {
				data = queue.poll(TIME_SLEEP, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			if(data != null) {
				break;
			}
			System.out.println("no data");
			
			if(finish && queue.size() == 0){
				System.out.println("task finish");
				break;
			}
		}
		return data;
	}
	
	public static void main(String[] args) {
		get();
	}
}
