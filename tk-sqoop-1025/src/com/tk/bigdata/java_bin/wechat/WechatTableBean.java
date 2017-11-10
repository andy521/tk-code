package com.tk.bigdata.java_bin.wechat;

import com.tk.bigdata.java_bin.em.Status;
import com.tk.bigdata.java_bin.utils.TimeoutDetection;

public class WechatTableBean {
	
	private String name;
	private String dbCount;
	private String exCount;
	private String time;
	private Status result;
	public WechatTableBean(String name, String dbCount, String exCount, String time, Status result) {
		this.name = name;
		this.dbCount = dbCount;
		this.exCount = exCount;
		this.time = time;
		this.result = result;
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
	public Status getResult() {
		return result;
	}
	public void setResult(Status result) {
		this.result = result;
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
		return String.format("\n当前时间：%1$s \n表空间：%2$s \n查询量：%3$s \n导出量：%4$s \n差值量：%5$s \n总耗时：%6$s \n执行状态：%7$s ", TimeoutDetection.formatter(System.currentTimeMillis()), name, dbCount, exCount, c, time, result.name());
	}
	
	
}
