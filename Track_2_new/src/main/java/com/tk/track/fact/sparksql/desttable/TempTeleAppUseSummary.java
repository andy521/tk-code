package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

public class TempTeleAppUseSummary implements Serializable{
	private static final long serialVersionUID = 5242589496279973504L;

	   private String USER_ID;
	   private String TERMINAL_ID;		//设备ID
	   private String STARTTIME;    	//开始时间
	   private String ENDTIME;     		//结束时间
	   private String DURATION;     	//持续时间
	
	   public TempTeleAppUseSummary(){}	    
	
	   public TempTeleAppUseSummary(String uSER_ID, String tERMINAL_ID,String sTARTTIME, String eNDTIME,String dURATION) {
		super();
		USER_ID = uSER_ID;
		TERMINAL_ID = tERMINAL_ID;
		STARTTIME = sTARTTIME;
		setENDTIME(eNDTIME);
		setDURATION(dURATION);
	}

	public String getUSER_ID() {
		return USER_ID;
	}

	public void setUSER_ID(String uSER_ID) {
		USER_ID = uSER_ID;
	}

	public String getTERMINAL_ID() {
		return TERMINAL_ID;
	}

	public void setTERMINAL_ID(String tERMINAL_ID) {
		TERMINAL_ID = tERMINAL_ID;
	}

	public String getSTARTTIME() {
		return STARTTIME;
	}

	public void setSTARTTIME(String sTARTTIME) {
		STARTTIME = sTARTTIME;
	}

	public String getENDTIME() {
		return ENDTIME;
	}

	public void setENDTIME(String eNDTIME) {
		ENDTIME = eNDTIME;
	}

	public String getDURATION() {
		return DURATION;
	}

	public void setDURATION(String dURATION) {
		DURATION = dURATION;
	}


	  
	   
	   
	  
}
