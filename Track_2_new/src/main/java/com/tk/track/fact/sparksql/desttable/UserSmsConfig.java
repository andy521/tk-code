package com.tk.track.fact.sparksql.desttable;

public class UserSmsConfig {
	
	private long smsId;
	private String fromId;
	private String fromName;
	private String actURL;
	private String detailURL;
	private String actName;
	private String actId;
	
	public long getSmsId() {
		return smsId;
	}
	public void setSmsId(long smsId) {
		this.smsId = smsId;
	}
	public String getFromId() {
		return fromId;
	}
	public void setFromId(String fromId) {
		this.fromId = fromId;
	}
	public String getFromName() {
		return fromName;
	}
	public void setFromName(String fromName) {
		this.fromName = fromName;
	}
	public String getActURL() {
		return actURL;
	}
	public void setActURL(String actURL) {
		this.actURL = actURL;
	}
	public String getActName() {
		return actName;
	}
	public void setActName(String actName) {
		this.actName = actName;
	}
	public String getActId() {
		return actId;
	}
	public void setActId(String actId) {
		this.actId = actId;
	}
	public String getDetailURL() {
		return detailURL;
	}
	public void setDetailURL(String detailURL) {
		this.detailURL = detailURL;
	}
	
}
