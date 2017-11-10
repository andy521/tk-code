package com.tk.track.vo;

import java.util.List;
import java.util.Map;

public class UserBehaviorDetailConfig {
	private long id;
	private String event;
	private String subtype;
	private String userType;
	private String label;
	private String firstLevel;
	private String secondLevel;
	private String thirdLevel;
	private String fourthLevel;
	private List<UserBehaviorSpecialDataConfigVo> specialConfigList;

	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public String getEvent() {
		return event;
	}
	public void setEvent(String event) {
		this.event = event;
	}
	public String getSubtype() {
		return subtype;
	}
	public void setSubtype(String subtype) {
		this.subtype = subtype;
	}
	public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}
	public String getFirstLevel() {
		return firstLevel;
	}
	public void setFirstLevel(String firstLevel) {
		this.firstLevel = firstLevel;
	}
	public String getSecondLevel() {
		return secondLevel;
	}
	public void setSecondLevel(String secondLevel) {
		this.secondLevel = secondLevel;
	}
	public String getThirdLevel() {
		return thirdLevel;
	}
	public void setThirdLevel(String thirdLevel) {
		this.thirdLevel = thirdLevel;
	}
	public String getFourthLevel() {
		return fourthLevel;
	}
	public void setFourthLevel(String fourthLevel) {
		this.fourthLevel = fourthLevel;
	}
	public List<UserBehaviorSpecialDataConfigVo> getSpecialConfigList() {
		return specialConfigList;
	}
	public void setSpecialConfigList(
			List<UserBehaviorSpecialDataConfigVo> specialConfigList) {
		this.specialConfigList = specialConfigList;
	}
	public String getUserType() {
		return userType;
	}
	public void setUserType(String userType) {
		this.userType = userType;
	}
	
}
