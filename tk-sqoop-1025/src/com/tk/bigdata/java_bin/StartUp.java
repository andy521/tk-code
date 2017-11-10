package com.tk.bigdata.java_bin;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import com.tk.bigdata.java_bin.test.Demo11_DBCount_HBCount;
import com.tk.bigdata.java_bin.test.Demo15_ConfigValidate;
import com.tk.bigdata.java_bin.test.Demo3;

public class StartUp {
	
	public static void main(String[] args) throws Exception {
		
		if(args.length == 0){
			tips();
		}
		
		String[] realParams = new String[args.length - 1];
		for (int i = 1; i < args.length; i++) {
			realParams[i - 1] = args[i];
		}
		
		String p = args[0];
		ClassType type = null;
		try {
			type = ClassType.valueOf(p);
		} catch (Exception e) {
			int index = 0;
			try {
				index = Integer.parseInt(p);
			} catch (Exception e2) {
				tips();
			}
			type = init().get(index);
			if(type == null) tips();
		}
		
		switch (type) {
		case EXPORT_DATA:
			Demo3.main(realParams);
			break;
		case COUNT_DB_HBASE:
			Demo11_DBCount_HBCount.main(realParams);
			break;
		case CONFIG_VALIDATE:
			Demo15_ConfigValidate.main(realParams);
			break;
		default:
			tips();
		}
		
	}
	
	private static Map<Integer, ClassType> init() {
		Map<Integer, ClassType> map = new HashMap<>();
		Field[] fields = ClassType.class.getFields();
		for (Field field : fields) {
			ClassType type = ClassType.valueOf(field.getName());
			map.put(type.index, type);
		}
		return map;
	}

	private static void tips(){
		System.out.println("请传入启动方式: ");
		Field[] fields = ClassType.class.getFields();
		for (Field field : fields) {
			ClassType type = ClassType.valueOf(field.getName());
			System.out.println(type);
		}
		
		System.exit(-1);
	}
	
	enum ClassType{
		/** 导数脚本 */
		EXPORT_DATA(1, "导数脚本", "@1:日志文件路径, @2:配置文件路径, @3:程序执行线程数量"),
		/** 统计DB中的数量和Hbase中的数量 */
		COUNT_DB_HBASE(2, "统计DB中的数量和Hbase中的数量", "@1:配置文件路径"),
		/** 验证配置文件的正确性 */
		CONFIG_VALIDATE(3, "验证配置文件的正确性", "@1:配置文件路径"),
		

		;
		private int index;
		private String desc;
		private String params;
		private ClassType(int index, String desc, String params) {
			this.index = index;
			this.desc = desc;
			this.params = params;
		}
		
		@Override
		public String toString(){
			return String.format("%s. %s(%s)[%s]", this.index, this.name(), this.desc, this.params);
		}
		
	}
	
}
