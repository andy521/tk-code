package com.taikang.bigdata.sqoop.wechat;

public class WechatTableBean {
	
	private String name;
	private String dbCount;
	private String exCount;
	private String time;
	public WechatTableBean(String name, String dbCount, String exCount, String time) {
		this.name = name;
		this.dbCount = dbCount;
		this.exCount = exCount;
		this.time = time;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDbCount() {
		return dbCount;
	}
	public void setDbCount(String dbCount) {
		this.dbCount = dbCount;
	}
	public String getExCount() {
		return exCount;
	}
	public void setExCount(String exCount) {
		this.exCount = exCount;
	}
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	@Override
	public String toString() {
		int c = 0;
		try {
			c = Integer.parseInt(dbCount) - Integer.parseInt(exCount);
		} catch (Exception e) {
			e.printStackTrace();
		}
//		return "\n表空间：" + name + "\n查询量：" + dbCount + "\n导出量：" + exCount + "\n差值量：" + c + "\n总耗时：" + time + "\n执行状态：" + result;
		return String.format("\n当前时间：%1$s \n表空间：%2$s \n查询量：%3$s \n导出量：%4$s \n差值量：%5$s \n总耗时：%6$s \n执行状态：%7$s ", name, dbCount, exCount, c, time);
	}
	
	
}
