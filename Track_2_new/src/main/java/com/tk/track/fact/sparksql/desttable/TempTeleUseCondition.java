package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

public class TempTeleUseCondition implements Serializable{
	private static final long serialVersionUID = 5242589496279973504L;

	   private String USER_ID;
	   private String VISIT_TIME;    //时间
	   private String APP_TYPE;
	   private String APP_ID;     
	   private String MAIN_MENU;      //主功能菜单
	   private String SUB_MENU;       //子功能菜单
	   private String FUNCTION_DESC;  //事件
	
	   public TempTeleUseCondition(){}	    
	
	   public TempTeleUseCondition(String uSER_ID, String vISIT_TIME,
			String aPP_TYPE, String aPP_ID, String mAIN_MENU, String sUB_MENU,
			String fUNCTION_DESC) {
		super();
		USER_ID = uSER_ID;
		VISIT_TIME = vISIT_TIME;
		APP_TYPE = aPP_TYPE;
		APP_ID = aPP_ID;
		MAIN_MENU = mAIN_MENU;
		SUB_MENU = sUB_MENU;
		FUNCTION_DESC = fUNCTION_DESC;
	}

	public String getUSER_ID() {
		return USER_ID;
	}

	public void setUSER_ID(String uSER_ID) {
		USER_ID = uSER_ID;
	}

	public String getVISIT_TIME() {
		return VISIT_TIME;
	}

	public void setVISIT_TIME(String vISIT_TIME) {
		VISIT_TIME = vISIT_TIME;
	}

	public String getAPP_TYPE() {
		return APP_TYPE;
	}

	public void setAPP_TYPE(String aPP_TYPE) {
		APP_TYPE = aPP_TYPE;
	}

	public String getAPP_ID() {
		return APP_ID;
	}

	public void setAPP_ID(String aPP_ID) {
		APP_ID = aPP_ID;
	}

	public String getMAIN_MENU() {
		return MAIN_MENU;
	}

	public void setMAIN_MENU(String mAIN_MENU) {
		MAIN_MENU = mAIN_MENU;
	}

	public String getSUB_MENU() {
		return SUB_MENU;
	}

	public void setSUB_MENU(String sUB_MENU) {
		SUB_MENU = sUB_MENU;
	}

	public String getFUNCTION_DESC() {
		return FUNCTION_DESC;
	}

	public void setFUNCTION_DESC(String fUNCTION_DESC) {
		FUNCTION_DESC = fUNCTION_DESC;
	}
	  
	   
	   
	  
}
