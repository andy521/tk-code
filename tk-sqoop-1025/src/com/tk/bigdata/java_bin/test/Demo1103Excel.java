package com.tk.bigdata.java_bin.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.yaml.snakeyaml.Yaml;

import com.tk.bigdata.java_bin.domain.DbBean;
import com.tk.bigdata.java_bin.domain.SqoopBean;
import com.tk.bigdata.java_bin.utils.ConfigUtils;
import com.tk.bigdata.java_bin.utils.SqoopProcessUtils;

public class Demo1103Excel {

	private static Map<String, DbBean> yamls = new HashMap<>();

	public static void main(String[] args) throws Exception {
		// File directory = new
		// File("C:\\Users\\itw_meisf\\Desktop\\tables_beform_all");

		File directory = new File(args[0]);

		if (!directory.exists()) {
			System.out.println("is not directory");
			return;
		}

		readFile(directory);
		createExcel();
		System.out.println("结束了，你可以查看文件了");
	}

	private static void createExcel() throws Exception {
		XSSFWorkbook workbook = new XSSFWorkbook();
		XSSFSheet sheet = workbook.createSheet();
		XSSFRow oneRow = sheet.createRow(0);
		oneRow.createCell(0).setCellValue("表名");
		oneRow.createCell(1).setCellValue("hbase数据集");
		oneRow.createCell(2).setCellValue("oracle数据集");
		int rowIndex = 1;

		for (Entry<String, DbBean> entry : yamls.entrySet()) {
			List<SqoopBean> ls = ConfigUtils.loadingConfig(entry.getValue());
			SqoopBean sqoopBean = ls.get(0);
			// 查询sql
			Connection conn = null;
			try {
				if (sqoopBean.getUrl().contains("oracle")) {
					conn = Demo7_JDBC.getConnectionOracle(sqoopBean.getUrl(),
							sqoopBean.getUsername(), sqoopBean.getPassword());
				} else {
					conn = Demo7_JDBC.getConnectionMysql(sqoopBean.getUrl(),
							sqoopBean.getUsername(), sqoopBean.getPassword());
				}
			} catch (Exception e) {
				System.out.println("获取数据库链接失败, 无法统计count数据, error: "
						+ e.getMessage());
			}

			for (int i = 0; i < ls.size(); i++) {
				SqoopBean sb = ls.get(i);
				System.out.println("hbaseName:" + sb.getHbaseTableName()
						+ "   tableName:" + sb.getTableName());

				XSSFRow row = sheet.createRow(rowIndex++);
				row.createCell(0).setCellValue(sb.getTableName());

				// 查询shell
				Process process = Runtime
						.getRuntime()
						.exec(new String[] {
								"/bin/sh",
								"-c",
								String.format(
										"echo \"scan '%s',{LIMIT=>10}\" |hbase shell",
										sb.getHbaseTableName().substring(
												0,
												sb.getHbaseTableName().length()
														- "_NEW".length())) });
				String shellOutput = printShell(process);
				StringBuffer oracleOutput = new StringBuffer();
				// 查询数据库
				if (conn != null) {
					String sql = SqoopProcessUtils.getCountSQL(sqoopBean);
					sql = sql.replace("SELECT count(0) SQL_COUNT FROM",
							"SELECT * FROM");
					// System.out.println("执行统计SQL:" + sql);

					ResultSet rs = conn.prepareStatement(sql).executeQuery();
					int size = 0;
					while (rs.next()) {
						rs.getConcurrency();
						for (int j = 0; j < rs.getMetaData().getColumnCount(); j++) {
							oracleOutput.append(rs.getString(j + 1)).append(
									"\t");
							// System.out.print(rs.getString(j + 1) + "\t");
						}
						// System.out.println();
						oracleOutput.append("\n");
						if (++size >= 10) {
							break;
						}
					}
				}

				row.createCell(1).setCellValue(
						shellOutput.length() > 32767 ? shellOutput.substring(0,
								32767) : shellOutput);
				row.createCell(2).setCellValue(
						oracleOutput.length() > 32767 ? oracleOutput.substring(
								0, 32767) : oracleOutput.toString());
			}
			conn.close();
		}

		FileOutputStream fos = new FileOutputStream(new File(
				"/home/tkexporter/data.xlsx"));
		workbook.write(fos);
		workbook.close();
		fos.close();
	}

	private static String printShell(Process process) throws IOException {
		StringBuffer sb = new StringBuffer();
		BufferedReader br = new BufferedReader(new InputStreamReader(
				process.getInputStream(), "UTF-8"));
		String line;
		while ((line = br.readLine()) != null) {
			sb.append(line).append("\n");
			// System.out.println(line);
		}
		br.close();
		return sb.toString();
	}

	private static void readFile(File directory) throws FileNotFoundException,
			IOException {
		File[] files = directory.listFiles();
		Yaml yaml = new Yaml();
		for (File f : files) {
			if (f.getName().endsWith(".yml")) {
				DbBean bean = yaml.loadAs(new FileInputStream(f), DbBean.class);
				yamls.put(bean.getSchemas().get(0), bean);
			}
		}
	}

}
