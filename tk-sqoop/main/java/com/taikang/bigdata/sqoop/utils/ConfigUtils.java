package com.taikang.bigdata.sqoop.utils;

import java.util.ArrayList;
import java.util.List;

import com.taikang.bigdata.sqoop.domain.DbBean;
import com.taikang.bigdata.sqoop.domain.SchemaBean;
import com.taikang.bigdata.sqoop.domain.LinkBean;
import com.taikang.bigdata.sqoop.domain.SqoopBean;
import com.taikang.bigdata.sqoop.domain.TableBean;

public class ConfigUtils {
	
	public static List<SqoopBean> loadingConfig(DbBean bean){
		List<SqoopBean> dataList = new ArrayList<>();
		//需要导入的数据库
		List<String> schemas = bean.getSchemas();
		List<SchemaBean> schemas_list = bean.getSchemas_list();
		
		if(Utils.isEmpty(schemas, schemas_list)) return dataList;
		
		for (SchemaBean schemaBean : schemas_list) {
			String schema = schemaBean.getSchema();
			if(schemas.contains(schema)){
				//验证数据库链接是否有效
				LinkBean linkBean = schemaBean.getLink();
				boolean dbStatus = ValidataDbConnection.validata(linkBean);
				//需要导入的表
				List<String> tables = schemaBean.getTables();
				List<TableBean> table_list = schemaBean.getTable_list();
				
				if(Utils.isEmpty(tables, table_list)) continue;
				
				for (TableBean tableBean : table_list) {
					String tableName = tableBean.getTable();
					if(tables.contains(tableName)){
						dataList.add(new SqoopBean(linkBean, dbStatus, schema, tableBean, dataList.size()));
					}
				}
			}
		}
		
		return dataList;
	}
	
}
