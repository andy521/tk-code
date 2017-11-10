package com.tk.bigdata.java_bin.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.yaml.snakeyaml.Yaml;

import com.mysql.fabric.xmlrpc.base.Array;
import com.tk.bigdata.java_bin.domain.DbBean;
import com.tk.bigdata.java_bin.domain.SchemaBean;
import com.tk.bigdata.java_bin.domain.TableBean;

public class Demo1103 {
	private static Map<String, DbBean> resultMap = new HashMap<>();
	private static Map<String, List<String>> resultShMap = new HashMap<>();
	private static Map<String, String> resultShNameMap = new HashMap<>();

	private static Map<String, String> map = new HashMap<>();
	private static int tablesSize = 0;

	static {
//		map.put("tkinsure.CS_LOG", null);
//		map.put("tkinsure.P_CUSTOMER", null);
//		map.put("tkinsure.P_LIFEINSURECOMP", null);;
//		map.put("tkctrip.P_INSURANT", null);
//		map.put("tkinsure.P_INSURANT", null);
		map.put("upiccore.GUPOLICYRELATEDPARTY", null);
		map.put("upiccore.GUPOLICYRISK", null);
//		map.put("upiccore.GUPOLICYCOPYRISK", null);
//		map.put("upiccore.GUPOLICYCOPYCOMMISSION", null);
//		map.put("upiccore.GUPOLICYMAIN", null);
//		map.put("tkinsure.T_INSURELIST", null);
//		map.put("upiccore.GUPOLICYCOPYMAIN", null);
//		map.put("upiccore.GUPOLICYCOPYITEMKIND", null);

	}

	public static void main(String[] args) throws Exception {
		File ymlPath = new File("C:\\Users\\itw_meisf\\Desktop\\tables_all_inc.yml");
		DbBean bean = (DbBean) new Yaml().load(new FileInputStream(ymlPath));

		System.out.println(map.size());

		findScheme(bean);
		saveSchemaFile();
		createShFile();

		System.out.println(tablesSize);
		System.out.println("剩余：" + map.size());
		Iterator<String> iterator = map.keySet().iterator();
		while (iterator.hasNext()) {
			System.out.println(iterator.next());
		}
	}

	private static void findScheme(DbBean bean) {
		DbBean result = null;
		List<SchemaBean> schemaBeans = bean.getSchemas_list();
		for (SchemaBean sb : schemaBeans) {
			List<TableBean> tbs = new ArrayList<>();
			List<String> tbNames = new ArrayList<>();
			List<String> hbNames = new ArrayList<>();
			for (TableBean t : sb.getTable_list()) {
				if (map.containsKey(t.getTable())) {
					map.remove(t.getTable());
					tbs.add(t);
					tbNames.add(t.getTable());
					hbNames.add(t.getHbase_table());
					t.setHbase_table(t.getHbase_table().trim() + "_NEW");
				}
			}
			if (tbs.size() <= 0) {
				continue;
			}
			tablesSize += tbs.size();

			//按库生成配置文件
			SchemaBean resultSb = new SchemaBean();
			resultSb = sb;
			resultSb.setTable_list(tbs);
			resultSb.setTables(tbNames);
			result = new DbBean();
			result.setSchemas(new ArrayList<String>());
			result.getSchemas().add(sb.getSchema());
			result.setSchemas_list(new ArrayList<SchemaBean>());
			result.getSchemas_list().add(resultSb);
			resultMap.put(sb.getSchema(), result);
			resultShMap.put(sb.getSchema(), hbNames);
			
			//按表生成配置文件
//			SchemaBean resultSb = null;
//			for(int i = 0;i<tbs.size();i++){
//				resultSb =  new SchemaBean();
//				resultSb.setLink(sb.getLink());
//				resultSb.setName(sb.getName());
//				resultSb.setSchema(sb.getSchema());
//				resultSb.setTable_list(new ArrayList<TableBean>());
//				TableBean tableBean = tbs.get(i);
//				tableBean.setWhere(null);
//				resultSb.getTable_list().add(tableBean);
//				resultSb.setTables(new ArrayList<String>());
//				resultSb.getTables().add(tbNames.get(i));
//				result = new DbBean();
//				result.setSchemas(new ArrayList<String>());
//				result.getSchemas().add(sb.getSchema());
//				result.setSchemas_list(new ArrayList<SchemaBean>());
//				result.getSchemas_list().add(resultSb);
//				resultMap.put(tbNames.get(i), result);
//				resultShNameMap.put(tbNames.get(i), hbNames.get(i));
//			}
		}
	}

	private static void saveSchemaFile() throws Exception {
		if (resultMap.size() <= 0) {
			return;
		}

		
		Yaml yaml = new Yaml();
		
		//按库生成配置文件
//		for (Entry<String, DbBean> entry : resultMap.entrySet()) {
//			try {
//				yaml.dump(entry.getValue(),
//						new FileWriter(String.format(
//								"C:\\Users\\itw_meisf\\Desktop\\tables_beform_all\\tables_%s_before_all.yml",
//								entry.getKey())));
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
		
//		//按表生成配置文件，一个表，一个配置文件
//		for (Entry<String, DbBean> entry : resultMap.entrySet()) {
//			try {
//				yaml.dump(entry.getValue(),
//						new FileWriter(String.format(
//								"C:\\Users\\itw_meisf\\Desktop\\tables_beform_all\\table_%s_before_all.yml",
//								entry.getKey())));
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}

//		生成一个配置文件
		 DbBean bean = new DbBean();
		 bean.setSchemas(new ArrayList());
		 bean.setSchemas_list(new ArrayList<SchemaBean>());
		 for (Entry<String, DbBean> entry : resultMap.entrySet()) {
			 bean.getSchemas().addAll(entry.getValue().getSchemas());
			 bean.getSchemas_list().addAll(entry.getValue().getSchemas_list());
		 }
		
		 yaml.dump(bean, new
		 FileWriter("C:\\Users\\itw_meisf\\Desktop\\tables_beform_all\\tables_complement_all.yml"));
	}


	private static void createShFile() throws Exception {
		
//		//按表生成命令文件
//		if (resultShNameMap.size() <= 0) {
//			return;
//		}
//		for (Entry<String, String> entry : resultShNameMap.entrySet()) {
//			 saveShellFile(entry.getKey() + "_create_hbase_new_table", String.format(
//						"echo \"disable '%1$s_NEW';drop '%1$s_NEW';create '%1$s_NEW', 'info', {SPLITS => ['1', '2', '3', '4', '5', '6','7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F']};exit;\"|hbase shell",
//						entry.getValue()));
////			 saveShellFile(entry.getKey() + "_update_hbase_old_table_name", String.format(
////						"echo \"disable '%1$s';snapshot '%1$s','tableSnapshot';clone_snapshot 'tableSnapshot','%1$s_20171108'; delete_snapshot 'tableSnapshot';drop '%1$s';exit;\"|hbase shell",
////						entry.getValue()));
////			 saveShellFile(entry.getKey() + "_update_hbase_new_table_name", String.format(
////						"echo \"disable '%1$s_NEW';snapshot '%1$s_NEW','tableSnapshot';clone_snapshot 'tableSnapshot','%1$s'; delete_snapshot 'tableSnapshot';drop '%1$s_NEW';exit;\"|hbase shell",
////						entry.getValue()));
//		 }
		

		
		
		if (resultShMap.size() <= 0) {
			return;
		}
		
		
//按库生成命令文件
//		 for (Entry<String, List<String>> entry : resultShMap.entrySet()) {
//			 List<String> createHbaseNewTableShells = new ArrayList<>();
//			 List<String> updateHbaseOldTableNameShells = new ArrayList<>();
//			 List<String> updateHbaseNewTableNameShells = new ArrayList<>();
//			
//			 for (String hbName : entry.getValue()) {
//					createHbaseNewTableShells.add(String.format(
//							"echo \"disable '%1$s_NEW';drop '%1$s_NEW';create '%1$s_NEW', 'info', {SPLITS => ['1', '2', '3', '4', '5', '6','7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F']};exit;\"|hbase shell",
//							hbName));
//					updateHbaseOldTableNameShells.add(String.format(
//							"echo \"disable '%1$s';snapshot '%1$s','tableSnapshot';clone_snapshot 'tableSnapshot','%1$s_20171108'; delete_snapshot 'tableSnapshot';drop '%1$s';exit;\"|hbase shell",
//							hbName));
//					updateHbaseNewTableNameShells.add(String.format(
//							"echo \"disable '%1$s_NEW';snapshot '%1$s_NEW','tableSnapshot';clone_snapshot 'tableSnapshot','%1$s'; delete_snapshot 'tableSnapshot';drop '%1$s_NEW';exit;\"|hbase shell",
//							hbName));
//				}
//			
//			 saveShellFile(entry.getKey() + "_create_hbase_new_tables", createHbaseNewTableShells);
//	//		 saveShellFile(entry.getKey() + "_update_hbase_old_tables_name", updateHbaseOldTableNameShells);
//	//		 saveShellFile(entry.getKey() + "_update_hbase_new_tables_name", updateHbaseNewTableNameShells);
//		 }

		 
		 //生成一个命令文件
		List<String> createHbaseNewTableShells = new ArrayList<>();
		List<String> updateHbaseOldTableNameShells = new ArrayList<>();
		List<String> updateHbaseNewTableNameShells = new ArrayList<>();
		for (Entry<String, List<String>> entry : resultShMap.entrySet()) {

			for (String hbName : entry.getValue()) {
				createHbaseNewTableShells.add(String.format(
						"echo \"disable '%1$s_NEW';drop '%1$s_NEW';create '%1$s_NEW', 'info', {SPLITS => ['1', '2', '3', '4', '5', '6','7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F']};exit;\"|hbase shell",
						hbName));
				updateHbaseOldTableNameShells.add(String.format(
						"echo \"disable '%1$s';snapshot '%1$s','tableSnapshot';clone_snapshot 'tableSnapshot','%1$s_20171108'; delete_snapshot 'tableSnapshot';drop '%1$s';exit;\"|hbase shell",
						hbName));
				updateHbaseNewTableNameShells.add(String.format(
						"echo \"disable '%1$s_NEW';snapshot '%1$s_NEW','tableSnapshot';clone_snapshot 'tableSnapshot','%1$s'; delete_snapshot 'tableSnapshot';drop '%1$s_NEW';exit;\"|hbase shell",
						hbName));
			}

		}
		// saveShellFile("create_hbase_new_tables", createHbaseNewTableShells);
		// saveShellFile("update_hbase_old_tables_name",
		// updateHbaseOldTableNameShells);
		// saveShellFile("update_hbase_new_tables_name",
		// updateHbaseNewTableNameShells);

		FileOutputStream fos = new FileOutputStream(
				String.format("C:\\Users\\itw_meisf\\Desktop\\tables_beform_all\\%s_hbase.sh", "update_hbase_tables_name"));
		for (int i = 0; i < updateHbaseNewTableNameShells.size(); i++) {
				fos.write(updateHbaseOldTableNameShells.get(i).getBytes());
				fos.write("\n".getBytes());
				fos.write(updateHbaseNewTableNameShells.get(i).getBytes());
				fos.write("\n".getBytes());
		}
		fos.flush();
		fos.close();
	}
	
	private static void saveShellFile(String tableName, String shell) throws FileNotFoundException, IOException {
		FileOutputStream fos = new FileOutputStream(
				String.format("C:\\Users\\itw_meisf\\Desktop\\tables_beform_all\\%s_hbase.sh", tableName));
			fos.write(shell.getBytes());
			fos.write("\n".getBytes());
		fos.flush();
		fos.close();
	}

	private static void saveShellFile(String schema, List<String> list) throws FileNotFoundException, IOException {
		FileOutputStream fos = new FileOutputStream(
				String.format("C:\\Users\\itw_meisf\\Desktop\\tables_beform_all\\%s_hbase.sh", schema));
		for (String s : list) {
			fos.write(s.getBytes());
			fos.write("\n".getBytes());
		}
		fos.flush();
		fos.close();
	}
}
