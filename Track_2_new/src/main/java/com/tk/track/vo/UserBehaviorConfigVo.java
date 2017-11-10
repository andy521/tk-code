package com.tk.track.vo;

import java.util.List;
import java.util.Map;

public class UserBehaviorConfigVo {
	
	private String appType;
	private String appId;
	private String functionDesc;
	private List<UserBehaviorDetailConfig> userBehaviorDetailConfigList;
	
	public String getAppType() {
		return appType;
	}
	public void setAppType(String appType) {
		this.appType = appType;
	}
	public String getAppId() {
		return appId;
	}
	public void setAppId(String appId) {
		this.appId = appId;
	}
	public String getFunctionDesc() {
		return functionDesc;
	}
	public void setFunctionDesc(String functionDesc) {
		this.functionDesc = functionDesc;
	}
	public List<UserBehaviorDetailConfig> getUserBehaviorDetailConfigList() {
		return userBehaviorDetailConfigList;
	}
	public void setUserBehaviorDetailConfigList(
			List<UserBehaviorDetailConfig> userBehaviorDetailConfigList) {
		this.userBehaviorDetailConfigList = userBehaviorDetailConfigList;
	}
	
}
