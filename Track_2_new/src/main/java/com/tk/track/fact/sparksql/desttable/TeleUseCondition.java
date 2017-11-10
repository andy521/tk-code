package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;



public class TeleUseCondition implements Serializable{
	private static final long serialVersionUID = 5242589436279973504L;
   private String ROWKEY;         //uuid 自动生成
   private String USER_ID;
   private String VISIT_TIME;     //时间
   private String MAIN_MENU;      //主功能菜单
   private String SUB_MENU;       //子功能菜单
   private String FUNCTION_DESC;  //事件
   private String CLICK_COUNT;    //点击次数
   
public TeleUseCondition(String rOWKEY, String uSER_ID, String vISIT_TIME,
		String mAIN_MENU, String sUB_MENU, String fUNCTION_DESC,
		String cLICK_COUNT) {
	super();
	ROWKEY = rOWKEY;
	USER_ID = uSER_ID;
	VISIT_TIME = vISIT_TIME;
	MAIN_MENU = mAIN_MENU;
	SUB_MENU = sUB_MENU;
	FUNCTION_DESC = fUNCTION_DESC;
	CLICK_COUNT = cLICK_COUNT;
}

public String getROWKEY() {
	return ROWKEY;
}

public void setROWKEY(String rOWKEY) {
	ROWKEY = rOWKEY;
}

public String getUSER_ID() {
	return USER_ID;
}

public void setUSER_ID(String uSE_ID) {
	USER_ID = uSE_ID;
}

public String getVISIT_TIME() {
	return VISIT_TIME;
}

public void setVISIT_TIME(String vISIT_TIME) {
	VISIT_TIME = vISIT_TIME;
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

public String getCLICK_COUNT() {
	return CLICK_COUNT;
}

public void setCLICK_COUNT(String cLICK_COUNT) {
	CLICK_COUNT = cLICK_COUNT;
}

   
   
}

