package com.tk.bigdata.java_bin.test;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class Demo10_ReadStruct {

	public static void main(String[] args) throws Exception {

		String path = "C:/Users/meisanfeng/Desktop/tables_structure(1).properties";

		Properties prop = new Properties();
		prop.load(new FileInputStream(path));

		Connection conn = Demo7_JDBC.getConnectionMysql(
				"jdbc:mysql:///bigdata?serverTimezone=UTC", "root", "root");
		QueryRunner runner = new QueryRunner();

		String sql = "INSERT INTO `bigdata`.`tables_struct` ("
				+ "`id`, `username`, `schema`, `table_name`, `column_name`, `data_type`, `data_length`, "
				+ "`data_scale`, `nullable`, `column_id`, `avg_col_len`"
				+ ") VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		Set<Object> keys = prop.keySet();
		for (Object key : keys) {
			JSONArray arr = JSONArray
					.parseArray(prop.getProperty((String) key));

			Object[][] params = new Object[arr.size()][];
			for (int i = 0; i < arr.size(); i++) {
				JSONObject json = arr.getJSONObject(i);
				String[] ss = key.toString().split("\\.");
				params[i] = new Object[] { ss[0], ss[1], ss[2],
						json.getString("COLUMN_NAME"),
						json.getString("DATA_TYPE"),
						json.getInteger("DATA_LENGTH"),
						json.getInteger("DATA_SCALE"),
						json.getString("NULLABLE"),
						json.getInteger("COLUMN_ID"),
						json.getInteger("AVG_COL_LEN") };
			}
			try {
				runner.insertBatch(conn, sql, new ScalarHandler<>(), params);
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}
		
		//
		// prop.forEach((key, v) -> {
		//
		// });

	}
}
