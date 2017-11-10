package com.taikang.bigdata.sqoop.utils;

import java.io.File;
import java.io.IOException;

public class FileUtils {
	
	/**
	 * 如果文件夹路径不存在, 则创建
	 */
	public static void validateFile(String path){
		if(path == null || "".equals(path)) return ;
		File f = new File(path);
		if(!f.exists()) f.mkdirs();
	}
	
	public static boolean exists(String path){
		return new File(path).exists();
	}
	
	public static File createFile(String path) throws IOException{
		File f = new File(path);
		if(f.exists()){
			return f;
		}
		f.createNewFile();
		return f;
	}
	
	public static void removeFile(String path, String status){
		new File(path).renameTo(new File(path + "-" + status));
//		new File(path).delete();
	}
	
}
