package com.tk.track.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeUtil {
	public static String getNowStr() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 设置日期格式
		return df.format(new Date());
	}

	public static Date string2Time(String str) {
		if (str != null) {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			df.setLenient(false);
			try {
				return df.parse(str);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	public static Date string2YMDHMSDate(String str) {
		if (str != null) {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			df.setLenient(false);
			try {
				return df.parse(str);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	public static Date string2YMDDate(String str) {
		if (str != null) {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			df.setLenient(false);
			try {
				return df.parse(str);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	public static String getYesStr() {
		Calendar cld = Calendar.getInstance();
		cld.setTime(new Date());
		cld.add(Calendar.DATE, -1);
		Date yes = cld.getTime();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");// 设置日期格式
		return df.format(yes);
	}
	public static String getBYStr() {
		Date dBefore = new Date();
		Calendar cale = Calendar.getInstance();
		cale.setTime(new Date());
		cale.add(Calendar.DAY_OF_MONTH, -2);
		dBefore = cale.getTime();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		String df = sdf.format(dBefore);
		return df;
	}
	public static String getNowStr(String partern) {
		SimpleDateFormat df = new SimpleDateFormat(partern);// 设置日期格式
		return df.format(new Date());
	}
}
