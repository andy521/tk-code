package com.taikang.bigdata.sqoop;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class StartUp {
	
	private static Map<ClassType, String> START_CLASS_MAP = null;
	static {
		START_CLASS_MAP = new HashMap<>();
		
		START_CLASS_MAP.put(ClassType.EXPORT_DATA, "com.taikang.bigdata.sqoop.test.Demo3");
		START_CLASS_MAP.put(ClassType.COUNT_DB_HBASE, "com.taikang.bigdata.sqoop.test.Demo_DBCount_HBCount");
	}
	
	
	public static void main(String[] args) throws Exception {
		
		String classname = START_CLASS_MAP.get(ClassType.EXPORT_DATA);
		
		Method method = Class.forName(classname).getMethod("main", String[].class);
		method.invoke(null, new Object[]{args});
	}
	
	enum ClassType{
		/** 导数脚本 */
		EXPORT_DATA,
		/** 统计DB中的数量和Hbase中的数量 */
		COUNT_DB_HBASE,
		
	}
	
}
