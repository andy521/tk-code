package com.tk.bigdata.java_bin.utils;

import java.io.File;
import java.io.IOException;

import com.tk.bigdata.java_bin.em.Status;

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
		return new File(path).exists() || new File(path+"-success").exists();
	}
	
	public static File createFile(String path) throws IOException{
		File f = new File(path);
		if(f.exists()){
			return f;
		}
		f.createNewFile();
		return f;
	}
	
	public static void removeFile(String path, Status status){
		new File(path).renameTo(new File(path + "-" + status.name()));
		if(status == Status.success){
			new File(path + "-" + Status.failed).delete();
		}
//		new File(path).delete();
	}
	
	
}
