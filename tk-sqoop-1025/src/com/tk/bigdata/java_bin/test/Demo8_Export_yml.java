package com.tk.bigdata.java_bin.test;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.yaml.snakeyaml.Yaml;

import com.tk.bigdata.java_bin.domain.DbBean;
import com.tk.bigdata.java_bin.domain.LinkBean;
import com.tk.bigdata.java_bin.domain.SchemaBean;
import com.tk.bigdata.java_bin.domain.TableBean;
import com.tk.bigdata.java_bin.utils.FileUtils;
import com.tk.bigdata.java_bin.utils.Utils;

public class Demo8_Export_yml {
	
	private static Map<String, String> sqlMap = new HashMap<String, String>();
	private static QueryRunner runner = new QueryRunner();
	private static Connection conn = null;
	
	private static String basicPath = "C:/Users/banzh/Desktop/tk-tables/";
	
	static {
		sqlMap.put("tables_others", "SELECT * FROM tables0923 WHERE tableName NOT IN('P_MEMBER', 'P_MEMBER_UPDATE', 'T_INSURELIST', 'T_INSURELIST_EC') AND dbName <> 'olbi';");
		sqlMap.put("tables_olbi", "SELECT * FROM tables0923 WHERE dbName = 'olbi';");
		sqlMap.put("tables_especial", "SELECT * FROM tables0923 WHERE tableName IN('P_MEMBER', 'P_MEMBER_UPDATE', 'T_INSURELIST', 'T_INSURELIST_EC');");

		try {
			conn = Demo7_JDBC.getConnectionMysql("jdbc:mysql:///bigdata?serverTimezone=UTC", "root", "123456");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		FileUtils.validateFile(basicPath+"tables/");
		FileUtils.validateFile(basicPath+"schemas/");
	}
	
	public static void main(String[] args) throws Exception {
//		getEspecial();
		getAll();
	}
	
	public static void getAll() throws Exception {

		
		List<Map<String, Object>> ls = runner.query(conn, "SELECT * FROM tables0923", new MapListHandler());
		
		DbBean dbBean = new DbBean();
		dbBean.setSchemas(new ArrayList<String>());
		dbBean.setSchemas_list(new ArrayList<SchemaBean>());
		
		for (Map<String, Object> map : ls) {
			String dbName = (String) map.get("dbName");
			String url = (String) map.get("url");
			String username = (String) map.get("username");
			String password = (String) map.get("password");
			
			String schema = (String) map.get("schema");
			String tableName = (String) map.get("tableName");
			if(!Utils.isEmpty(schema)){
				tableName = schema + "." + tableName;
			}
			
			
			String whereSQL = (String) map.get("where_new");
//			String whereSQL = (String) map.get("where_time");
			String row_key = (String) map.get("row_key");
			String split_by = (String) map.get("split_by");
			String add_row_key = (String) map.get("add_row_key");
			String num_mappers = (String) map.get("num_mappers");
			String column_family = (String) map.get("column_family");
			String columns = (String) map.get("colums");
			String workType = (String) map.get("workType");
			String before_import = (String) map.get("before_import");
			
			if(!dbBean.getSchemas().contains(dbName))
				dbBean.getSchemas().add(dbName);
			
			SchemaBean schemaBean = null;
			for (SchemaBean b : dbBean.getSchemas_list()) {
				if(dbName.equals(b.getSchema())){
					schemaBean = b;
					break;
				}
			}
			if(schemaBean == null){
				schemaBean = new SchemaBean();
				schemaBean.setSchema(dbName);
				schemaBean.setTables(new ArrayList<String>());
				schemaBean.setTable_list(new ArrayList<TableBean>());
				
				LinkBean link = new LinkBean();
				link.setDriver("");
				link.setUrl(url);
				link.setUsername(username);
				link.setPassword(password);
				schemaBean.setLink(link);
				
				dbBean.getSchemas_list().add(schemaBean);
			}
			
			schemaBean.getTables().add(tableName);
			
			TableBean tableBean = new TableBean();
			tableBean.setAdd_row_key(add_row_key);
			tableBean.setColumn_family(column_family);
			tableBean.setColumns(columns);
			tableBean.setHbase_table(workType);
			tableBean.setNum_mappers(num_mappers == null ? 4:Integer.parseInt(num_mappers));
			tableBean.setRow_key(row_key);
			tableBean.setSplit_by(split_by);
			tableBean.setWhere(whereSQL);
			tableBean.setTable(tableName);
			tableBean.setBefore_import(before_import);
			
			schemaBean.getTable_list().add(tableBean);
		}
		
		Yaml yaml = new Yaml();
		yaml.dump(dbBean, new OutputStreamWriter(new FileOutputStream(basicPath + "tables.yml")));
		
		
		{
			List<SchemaBean> schemas_list = dbBean.getSchemas_list();
			for (SchemaBean schemaBean : schemas_list) {
				String schema = schemaBean.getSchema();
				DbBean b = new DbBean();
				b.setSchemas(new ArrayList<String>());
				b.setSchemas_list(new ArrayList<SchemaBean>());
				
				b.getSchemas().add(schema);
				b.getSchemas_list().add(schemaBean);
				
				yaml.dump(b, new OutputStreamWriter(new FileOutputStream(basicPath + "schemas/tables_"+schema+".yml")));
			}
		}
		
		
		{
			List<SchemaBean> schemas_list = dbBean.getSchemas_list();
			for (SchemaBean schemaBean : schemas_list) {
				List<TableBean> table_list = schemaBean.getTable_list();
				String schema = schemaBean.getSchema();
				for (TableBean tableBean : table_list) {
					SchemaBean sbean = new SchemaBean();
					sbean.setSchema(schema);
					sbean.setLink(schemaBean.getLink());
					sbean.setTables(new ArrayList<String>());
					sbean.setTable_list(new ArrayList<TableBean>());
					sbean.getTables().add(tableBean.getTable());
					sbean.getTable_list().add(tableBean);
					
					DbBean b = new DbBean();
					b.setSchemas(new ArrayList<String>());
					b.setSchemas_list(new ArrayList<SchemaBean>());
					
					b.getSchemas().add(schema);
					b.getSchemas_list().add(sbean);
					yaml.dump(b, new OutputStreamWriter(new FileOutputStream(basicPath + "tables/table_"+schema+"_"+tableBean.getTable()+".yml")));
				}
			}
		}
	}
	
	public static void getEspecial() throws Exception{
		
		for (Entry<String, String> enntry : sqlMap.entrySet()) {
			String path = enntry.getKey();
			String sql = enntry.getValue();
			
			List<Map<String, Object>> ls = runner.query(conn, sql, new MapListHandler());
			
			DbBean dbBean = new DbBean();
			dbBean.setSchemas(new ArrayList<String>());
			dbBean.setSchemas_list(new ArrayList<SchemaBean>());
			
			for (Map<String, Object> map : ls) {
				String dbName = (String) map.get("dbName");
				String url = (String) map.get("url");
				String username = (String) map.get("username");
				String password = (String) map.get("password");
				
				String schema = (String) map.get("schema");
				String tableName = (String) map.get("tableName");
				if(!Utils.isEmpty(schema)){
					tableName = schema + "." + tableName;
				}
				
				
//				String whereSQL = (String) map.get("where");
				String whereSQL = (String) map.get("where_new");
				if(whereSQL == null || "".equals(whereSQL)) continue;
				String row_key = (String) map.get("row_key");
				String split_by = (String) map.get("split_by");
				String add_row_key = (String) map.get("add_row_key");
				String num_mappers = (String) map.get("num_mappers");
				String column_family = (String) map.get("column_family");
				String columns = (String) map.get("colums");
				String workType = (String) map.get("workType");
				String before_import = (String) map.get("before_import");
				
				if(!dbBean.getSchemas().contains(dbName))
					dbBean.getSchemas().add(dbName);
				
				SchemaBean schemaBean = null;
				for (SchemaBean b : dbBean.getSchemas_list()) {
					if(dbName.equals(b.getSchema())){
						schemaBean = b;
						break;
					}
				}
				if(schemaBean == null){
					schemaBean = new SchemaBean();
					schemaBean.setSchema(dbName);
					schemaBean.setTables(new ArrayList<String>());
					schemaBean.setTable_list(new ArrayList<TableBean>());
					
					LinkBean link = new LinkBean();
					link.setDriver("");
					link.setUrl(url);
					link.setUsername(username);
					link.setPassword(password);
					schemaBean.setLink(link);
					
					dbBean.getSchemas_list().add(schemaBean);
				}
				
				schemaBean.getTables().add(tableName);
				
				TableBean tableBean = new TableBean();
				tableBean.setAdd_row_key(add_row_key);
				tableBean.setColumn_family(column_family);
				tableBean.setColumns(columns);
				tableBean.setHbase_table(workType);
				tableBean.setNum_mappers(num_mappers == null ? 4:Integer.parseInt(num_mappers));
				tableBean.setRow_key(row_key);
				tableBean.setSplit_by(split_by);
				tableBean.setWhere(whereSQL);
				tableBean.setTable(tableName);
				tableBean.setBefore_import(before_import);
				
				schemaBean.getTable_list().add(tableBean);
			}
			
			Yaml yaml = new Yaml();
			yaml.dump(dbBean, new OutputStreamWriter(new FileOutputStream("C:/Users/meisanfeng/Desktop/tk-tables/"+path+".yml")));
		}
		
	}
	
	
}
