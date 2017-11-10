package com.tk.bigdata.java_bin.domain;

import com.tk.bigdata.java_bin.em.Status;

public class LogBean {
	/**库名+表名*/
	private String name;
	/**库名*/
	private String schema;
	/**表明*/
	private String tableName;

	/**开始时间*/
	private long startTime;
	/**结束时间*/
	private long endTime;
	/**总运行时间*/
	private long execTime;

	/**查询db表中数据总条数*/
	private String queryCount;
	/**查询开始时间*/
	private long queryStartTime;
	/**查询结束时间*/
	private long queryEndTime;
	/**查询总运行时间*/
	private long queryExecTime;

	/**导出数据条数*/
	private String exportCount;
	/**导出开始时间*/
	private long exportStartTime;
	/**导出结束时间*/
	private long exportEndTime;
	/**导出总运行时间*/
	private long exportExecTime;

	/**导数运行状态*/
	private Status result;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public long getStartTime() {
		return startTime;
	}
	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}
	public long getEndTime() {
		return endTime;
	}
	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}
	public long getExecTime() {
		return execTime;
	}
	public void setExecTime(long execTime) {
		this.execTime = execTime;
	}
	public String getQueryCount() {
		return queryCount;
	}
	public void setQueryCount(String queryCount) {
		this.queryCount = queryCount;
	}
	public long getQueryStartTime() {
		return queryStartTime;
	}
	public void setQueryStartTime(long queryStartTime) {
		this.queryStartTime = queryStartTime;
	}
	public long getQueryEndTime() {
		return queryEndTime;
	}
	public void setQueryEndTime(long queryEndTime) {
		this.queryEndTime = queryEndTime;
	}
	public long getQueryExecTime() {
		return queryExecTime;
	}
	public void setQueryExecTime(long queryExecTime) {
		this.queryExecTime = queryExecTime;
	}
	public String getExportCount() {
		return exportCount;
	}
	public void setExportCount(String exportCount) {
		this.exportCount = exportCount;
	}
	public long getExportStartTime() {
		return exportStartTime;
	}
	public void setExportStartTime(long exportStartTime) {
		this.exportStartTime = exportStartTime;
	}
	public long getExportEndTime() {
		return exportEndTime;
	}
	public void setExportEndTime(long exportEndTime) {
		this.exportEndTime = exportEndTime;
	}
	public long getExportExecTime() {
		return exportExecTime;
	}
	public void setExportExecTime(long exportExecTime) {
		this.exportExecTime = exportExecTime;
	}
	public Status getResult() {
		return result;
	}
	public void setResult(Status result) {
		this.result = result;
	}
	
	public static String getColumnName() {
		return "name|schema|tableName|startTime|endTime|execTime|queryCount|queryStartTime|queryEndTime|queryExecTime|exportCount|exportStartTime|exportEndTime|exportExecTime|result";
	}
	
	@Override
	public String toString() {
		return name + "|" + schema + "|" + tableName + "|" + startTime + "|" + endTime + "|" + (execTime / 1000.0 + " s") + "|"
				+ queryCount + "|" + queryStartTime + "|" + queryEndTime + "|" + (queryExecTime / 1000.0 + " s") + "|" + exportCount + "|"
				+ exportStartTime + "|" + exportEndTime + "|" + (exportExecTime / 1000.0 + " s") + "|" + result;
	}
	
	

}
