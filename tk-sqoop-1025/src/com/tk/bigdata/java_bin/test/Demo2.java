package com.tk.bigdata.java_bin.test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public class Demo2 {
	
	
	
	public static void main(String[] args) throws Exception {
		
		List<String> ls = new ArrayList<>();
		ls.add("[");
		ls.add("]");
		ls.add("{");
		ls.add("}");
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("C:/Users/meisanfeng/Desktop/data_js.json"), "UTF-8"));
		String line = "";
		StringBuilder sb = new StringBuilder("");
		while((line = br.readLine()) != null){
			if(line.startsWith("//")) continue;
			line = line.trim();
			if(ls.contains(line)){
				
			}else{
				line = "\"" + line + "\"";
				line = line.replace(":", "\":\"").replace(",", "\",\"").replace(":", "\":\"");
			}
			sb.append(line).append("\r\n");
		}
		br.close();
		
		System.out.println(sb.toString());
		
		ScriptEngineManager manager = new ScriptEngineManager();
	    ScriptEngine engine = manager.getEngineByName("js");
	    Object eval = engine.eval("JSON.stringify(eval('('+"+sb.toString()+"+')'))");
	    System.out.println(eval);
		
	}
	
}
