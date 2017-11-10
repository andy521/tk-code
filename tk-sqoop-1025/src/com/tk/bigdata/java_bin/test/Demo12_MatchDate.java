package com.tk.bigdata.java_bin.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.tk.bigdata.java_bin.em.SqlType;

public class Demo12_MatchDate {
	
	public static void main(String[] args) throws ParseException {
		SqlType type = SqlType.ORACLE;
		String str = "INPUT_DATE >= $sysday(-2) and INPUT_DATE < $sysday(-1)";
		System.out.println(getWhereSql(str, type));
	}
	
	private static Date date = null;
	static {
		try {
			date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(new SimpleDateFormat("yyyy-MM-dd").format(new Date()) + " 00:00:00");
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	public static String getWhereSql(String whereSql, SqlType type){
		Calendar calendar = Calendar.getInstance();
		Pattern pattern = Pattern.compile("\\$sysday\\([-]?[0-9]+\\)");
		Matcher matcher = pattern.matcher(whereSql);
		while(matcher.find()){
			String group = matcher.group();
			
			calendar.setTime(date);
			calendar.add(Calendar.DAY_OF_YEAR, Integer.parseInt(getNum(group)));
			String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
			whereSql = whereSql.replace(group, getSqlDate(time, type));
		}
		return whereSql;
	}
	
	private static String getSqlDate(String time, SqlType type){
		if(SqlType.ORACLE == type){
			return "TO_DATE('" + time + "', 'yyyy-mm-dd hh24:mi:ss')";
		}else if(SqlType.MYSQL == type){
			return "STR_TO_DATE('" + time + "', '%Y-%m-%d %T')";
		}
		return null;
	}
	
	private static String getNum(String str){
		Matcher matcher = Pattern.compile("[-]?[0-9]+").matcher(str);
		if(matcher.find()){
			return matcher.group();
		}
		return null;
	}
	
}
