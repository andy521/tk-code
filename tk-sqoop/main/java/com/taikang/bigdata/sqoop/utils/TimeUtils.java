package com.taikang.bigdata.sqoop.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeUtils {
	
	public static String getTimeByDay(){
		return new SimpleDateFormat("yyyy-MM-dd").format(new Date());
	}
	
	public static String getTimeByNow(){
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
	}
	
	public static String getTimeByTimes(){
		return new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
	}
	
}
