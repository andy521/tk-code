package com.taikang.bigdata.sqoop.utils;

import java.util.Collection;

public class Utils {
	
	public static boolean isEmpty(String str){
		return str == null || "".equals(str);
	}

	public static boolean isEmpty(String ... arr){
		if(arr == null || arr.length == 0) return true;
		for (String str : arr) {
			if(isEmpty(str)) return true;
		}
		return false;
	}
	
	public static boolean isEmpty(Collection<?> collection){
		return collection == null ||collection.size() == 0;
	}
	
	public static boolean isEmpty(Collection<?> ... collections){
		if(collections == null || collections.length == 0) return true;
		for (Collection<?> collection : collections) {
			if(isEmpty(collection)) return true;
		}
		return false;
	}
	
}
