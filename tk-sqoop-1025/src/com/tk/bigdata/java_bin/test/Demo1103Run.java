package com.tk.bigdata.java_bin.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.yaml.snakeyaml.Yaml;

import com.tk.bigdata.java_bin.domain.DbBean;
import com.tk.bigdata.java_bin.domain.TableBean;

public class Demo1103Run {
	private static List<String> sortTables = new ArrayList<>();
	
	private static Map<String, DbBean> yamls = new HashMap<>();
	private static Map<String, List<String>> createHbaseNewTableShellMap = new HashMap<>();
//	private static Map<String, List<String>> updateHbaseOldTableNameShellMap = new HashMap<>();
//	private static Map<String, List<String>> updateHbaseNewTableNameShellMap = new HashMap<>();

	static{
		sortTables.add("tkinsure.CS_LOG");
		sortTables.add("tkinsure.P_CUSTOMER");
		sortTables.add("tkinsure.P_LIFEINSURECOMP");;
		sortTables.add("tkctrip.P_INSURANT");
		sortTables.add("tkinsure.P_INSURANT");
		sortTables.add("upiccore.GUPOLICYRELATEDPARTY");
		sortTables.add("upiccore.GUPOLICYRISK");
		sortTables.add("upiccore.GUPOLICYCOPYRISK");
		sortTables.add("upiccore.GUPOLICYCOPYCOMMISSION");
		sortTables.add("upiccore.GUPOLICYMAIN");
		sortTables.add("tkinsure.T_INSURELIST");
		sortTables.add("upiccore.GUPOLICYCOPYMAIN");
		sortTables.add("upiccore.GUPOLICYCOPYITEMKIND");
	}
	
	public static void main(String[] args) throws Exception {

//		 File directory = new
//		 File("C:\\Users\\itw_meisf\\Desktop\\tables_beform_all");
		File directory = new File(args[0]);

		if (!directory.exists()) {
			System.out.println("is not directory");
			return;
		}

		readFile(directory);
		
//		System.out.println(yamls.size());
//		System.out.println(createHbaseNewTableShellMap.size());

		run();
		
//		Demo1103Excel.main(args);

	}

	private static void run() throws Exception {
		//按照既定顺序运行
		for(String tableName:sortTables){
			if(!yamls.containsKey(tableName)){
				continue;
			}
			if(!createHbaseNewTableShellMap.containsKey(tableName)){
				continue;
			}
			
			// 第一步，创建临时表
			Process process = Runtime.getRuntime().exec(
					new String[] { "/bin/sh", "-c", createHbaseNewTableShellMap.get(tableName).get(0) });
			process.waitFor();
			printShell(process);
//			System.out.println(createHbaseNewTableShellMap.get(tableName).get(0));
			
			
			// 往临时表导数
			Demo3.main(new String[] {
					"/home/tkexporter/log_new/",
					String.format(
							"/home/tkexporter/tables_beform_all/table_%s_before_all.yml",
							tableName), "1",
					"/home/tkexporter/wechat.config.test" });
//			System.out.println(String.format(
//							"/home/tkexporter/tables_beform_all/table_%s_before_all.yml",
//							tableName));
		
		}
		
		
		
		//随机按库运行
//		for (Entry<String, DbBean> entry : yamls.entrySet()) {
//			List<TableBean> beans = entry.getValue().getSchemas_list().get(0)
//					.getTable_list();
//			List<String> createHbaseNewTableShells = createHbaseNewTableShellMap
//					.get(entry.getKey());
////			List<String> updateHbaseOldTableNameShells = updateHbaseOldTableNameShellMap
////					.get(entry.getKey());
////			List<String> updateHbaseNewTableNameShells = updateHbaseNewTableNameShellMap
////					.get(entry.getKey());
//
//			if (beans.size() != createHbaseNewTableShells.size()
//					/*|| beans.size() != updateHbaseOldTableNameShells.size()
//					|| beans.size() != updateHbaseNewTableNameShells.size()*/) {
//				continue;
//			}
//
//			// 第一步，创建临时表
//			for (String shell : createHbaseNewTableShells) {
//				Process process = Runtime.getRuntime().exec(
//						new String[] { "/bin/sh", "-c", shell });
//				process.waitFor();
//				printShell(process);
//		       
//			}
//
//			// 往临时表导数
//			Demo3.main(new String[] {
//					"/home/tkexporter/log_new/",
//					String.format(
//							"/home/tkexporter/tables_beform_all/tables_%s_before_all.yml",
//							entry.getKey()), "4",
//					"/home/tkexporter/wechat.config.test" });
//			
////			// 将正式表进行备份
////			for (String shell : updateHbaseOldTableNameShells) {
////				Process process = Runtime.getRuntime().exec(
////						new String[] { "/bin/sh", "-c", shell });
////				process.waitFor();
////				printShell(process);
////			}
////
////			
////			// 将临时表改为正式表
////			for (String shell : updateHbaseNewTableNameShells) {
////				Process process = Runtime.getRuntime().exec(
////						new String[] { "/bin/sh", "-c", shell });
////				process.waitFor();
////				printShell(process);
////			}
//		}

	}

	private static void printShell(Process process) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream(), "UTF-8"));  
		String line;
		while ((line = br.readLine()) != null) {  
			System.out.println(line+"\n");
		}
		br.close();
	}

	private static void readFile(File directory) throws FileNotFoundException,
			IOException {
		//按库运行
//		File[] files = directory.listFiles();
//		Yaml yaml = new Yaml();
//		for (File f : files) {
//			if (f.getName().endsWith(".yml")) {
//				DbBean bean = yaml.loadAs(new FileInputStream(f), DbBean.class);
//				yamls.put(bean.getSchemas().get(0), bean);
//			} else {
//				String schema = f.getName().substring(0,
//						f.getName().indexOf("_"));
//				List<String> list = readShellFile(f);
//
//				if (f.getName().contains("_create_")) {
//					createHbaseNewTableShellMap.put(schema, list);
//				} /*else if (f.getName().contains("_update_")
//						&& f.getName().contains("_old_")) {
//					updateHbaseOldTableNameShellMap.put(schema, list);
//				} else {
//					updateHbaseNewTableNameShellMap.put(schema, list);
//				}*/
//			}
//		}
		
		
		
		//按表运行

		File[] files = directory.listFiles();
		Yaml yaml = new Yaml();
		for (File f : files) {
			if (f.getName().endsWith(".yml")) {
				DbBean bean = yaml.loadAs(new FileInputStream(f), DbBean.class);
				yamls.put(bean.getSchemas_list().get(0).getTables().get(0), bean);
			} else {
				List<String> list = readShellFile(f);

				if (f.getName().contains("_create_")) {
					createHbaseNewTableShellMap.put(f.getName().substring(0,
							f.getName().indexOf("_create_")), list);
				} /*else if (f.getName().contains("_update_")
						&& f.getName().contains("_old_")) {
					updateHbaseOldTableNameShellMap.put(schema, list);
				} else {
					updateHbaseNewTableNameShellMap.put(schema, list);
				}*/
			}
		}
		
		
		
	}

	private static List<String> readShellFile(File f)
			throws FileNotFoundException, IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream(f), "UTF-8"));
		List<String> list = new ArrayList<String>();
		String line = "";
		while ((line = br.readLine()) != null) {
			if (!"".equals(line)) {
				list.add(line);
			}
		}
		br.close();
		return list;
	}

}
