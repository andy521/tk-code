package com.taikang.bigdata.sqoop.utils;

import java.io.IOException;

import com.taikang.bigdata.sqoop.domain.SqoopBean;

public class SqoopProcessUtils {
	
	public static String renderCommond(SqoopBean bean) throws IOException{
		StringBuffer sb = new StringBuffer("");
		sb.append("sqoop ")
		  .append("import ")
		  .append("--connect '")
		  .append(bean.getUrl()).append("' ")
		  .append("--username ")
		  .append(bean.getUsername()).append(" ")
		  .append("--password ")
		  .append(bean.getPassword()).append(" ")
//		  .append("--table ")
//		  .append(bean.getTableName()).append(" ")
		  
		  .append("--query '")
		  .append(bean.getWhere()).append(" WHERE $CONDITIONS' ")
		  .append("--split-by '")
		  .append(bean.getSplitBy()).append("' ")
		  
		  
		  .append("--hbase-table ")
		  .append(bean.getHbaseTableName()).append(" ")
		  .append("--column-family ")
		  .append(bean.getColumnFamily()).append(" ")
		  .append("--hbase-row-key '")
		  .append(bean.getRowKey()).append("' ")
		  .append("-hbase-create-table ")
		  
		  //处理一下日志输出的位置
		  //预处理一下commond命令, 记录sys的log位置
		  .append("1>")
		  .append(ProcessUtils.SYS_PATH)
		  .append(TimeUtils.getTimeByDay())
		  .append("/")
//		  .append(System.currentTimeMillis())
//		  .append("-")
		  .append(bean.getName())
		  .append(".sys")
		  .append(" 2>&1")
		;
		FileUtils.validateFile(ProcessUtils.SYS_PATH + TimeUtils.getTimeByDay() + "/");
		FileUtils.createFile(ProcessUtils.SYS_PATH + TimeUtils.getTimeByDay() + "/" + bean.getName() + ".sys");
		
		return sb.toString();
	}
	
}
