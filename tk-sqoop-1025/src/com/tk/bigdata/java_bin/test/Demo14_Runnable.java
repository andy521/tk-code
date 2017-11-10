package com.tk.bigdata.java_bin.test;

import java.util.Random;

public class Demo14_Runnable implements Runnable {
	private String name;
	
	public Demo14_Runnable(String name) {
		this.name = name;
	}

	@Override
	public void run() {
		try {
			Thread.sleep(((new Random().nextInt(2) + 1) * 1000));
			System.out.println(this.name);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
