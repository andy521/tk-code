package com.tk.bigdata.java_bin.test;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Demo5 {
	
	public static void main(String[] args) throws Exception {
		System.out.println("开始执行~~");
		
		Process process = Runtime.getRuntime().exec(new String[]{});
		
		System.out.println(process.waitFor());
		StringBuffer sb = new StringBuffer("");
		BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));  
        String line;
        while ((line = br.readLine()) != null) {  
            sb.append(line).append("\r\n");
        }
        br.close();
        
        System.out.println(sb.toString());
        System.out.println("执行over~~");
	}
	
}
