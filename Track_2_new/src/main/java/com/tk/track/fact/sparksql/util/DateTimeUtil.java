package com.tk.track.fact.sparksql.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DateTimeUtil {
	
	public static long getTodayTime(int day) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, day);
		String yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		long timeStamp = 0;
		try {
			timeStamp = sdf.parse(yesterday).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return timeStamp;
	}
    
	public static String getSysDate(int day) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, day);
		String dateStr = new SimpleDateFormat("yyyyMMdd").format(cal.getTime());
		return dateStr;
	}

	/**
	 * GetDate
	 * @param day  day diff
	 * @return yyyyMMdd
	 */
	public static String GetSysDate(int day) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, day);
		String dateStr = new SimpleDateFormat("yyyyMMdd").format(cal.getTime());
		return dateStr;
	}
}
