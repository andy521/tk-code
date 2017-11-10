package com.tk.bigdata.java_bin.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Demo13_Thread {
	
	public static void main(String[] args) throws InterruptedException {
		
		ExecutorService executorService  = Executors.newFixedThreadPool(10); 
		for (int i = 0; i < 20; i++) {
			executorService.submit(new Demo14_Runnable(i + ""));
		}
		
		/*while(list.size() > 0){
			Iterator<Future<?>> it = list.iterator();
			while(it.hasNext()){
				if(it.next().isDone()){
					it.remove();
				}
			}
			Thread.sleep(1000);
			System.out.println("休息了一次");
		}
		*/
		executorService.shutdown();
		
		try {              
			boolean loop = true;              
			do {    
				//等待所有任务完成                  
				loop = !executorService.awaitTermination(2, TimeUnit.SECONDS);  
				//阻塞，直到线程池里所有任务结束            
				System.out.println("阻塞中");
			} while(loop);          
		} catch (InterruptedException e) {              
			e.printStackTrace();          
		} 
		
		System.out.println("12312313");
		
	}
	
}
