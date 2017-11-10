package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

public class TempTeleShareSummary implements Serializable{
	private static final long serialVersionUID = 5242589496279973504L;

	   private String USER_ID;
	   private String SHARETYPE;		//分享类型 '1'好友 '2'朋友圈 '3'在线APP
	   private String SHARETIME;     	//分享时间
	   private String SHARECONTENT;  	//分享内容 'planShare'计划\'seedShare'种子
	
	   public TempTeleShareSummary(){}	    
	
	   public TempTeleShareSummary(String uSER_ID, String sHARETYPE,String sHARETIME,String sHARECONTENT) {
		super();
		USER_ID = uSER_ID;
		SHARETYPE=sHARETYPE;
		SHARETIME=sHARETIME;
		SHARECONTENT=sHARECONTENT;
	}

	public String getUSER_ID() {
		return USER_ID;
	}

	public void setUSER_ID(String uSER_ID) {
		USER_ID = uSER_ID;
	}

	public String getSHARETYPE() {
		return SHARETYPE;
	}

	public void setSHARETYPE(String sHARETYPE) {
		SHARETYPE = sHARETYPE;
	}

	public String getSHARETIME() {
		return SHARETIME;
	}

	public void setSHARETIME(String sHARETIME) {
		SHARETIME = sHARETIME;
	}

	public String getSHARECONTENT() {
		return SHARECONTENT;
	}

	public void setSHARECONTENT(String sHARECONTENT) {
		SHARECONTENT = sHARECONTENT;
	}


	  
	   
	   
	  
}
