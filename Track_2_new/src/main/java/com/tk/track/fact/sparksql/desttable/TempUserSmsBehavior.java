package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

public class TempUserSmsBehavior implements Serializable{

	private String SMS_ID;
	private String USER_ID;		
	private String USER_TYPE;    	
	private String LAST_VISITTIME;  
	private String EVENT_TYPE;  
	private String DURATION;   
	
	public TempUserSmsBehavior(){}	    
	
	public TempUserSmsBehavior(String sMS_ID, String uSER_ID, String uSER_TYPE,
			String lAST_VISITTIME, String eVENT_TYPE, String dURATION) {
		super();
		SMS_ID = sMS_ID;
		USER_ID = uSER_ID;
		USER_TYPE = uSER_TYPE;
		LAST_VISITTIME = lAST_VISITTIME;
		EVENT_TYPE = eVENT_TYPE;
		DURATION = dURATION;
	}

	public String getSMS_ID() {
		return SMS_ID;
	}

	public void setSMS_ID(String sMS_ID) {
		SMS_ID = sMS_ID;
	}

	public String getUSER_ID() {
		return USER_ID;
	}

	public void setUSER_ID(String uSER_ID) {
		USER_ID = uSER_ID;
	}

	public String getUSER_TYPE() {
		return USER_TYPE;
	}

	public void setUSER_TYPE(String uSER_TYPE) {
		USER_TYPE = uSER_TYPE;
	}

	public String getLAST_VISITTIME() {
		return LAST_VISITTIME;
	}

	public void setLAST_VISITTIME(String lAST_VISITTIME) {
		LAST_VISITTIME = lAST_VISITTIME;
	}

	public String getEVENT_TYPE() {
		return EVENT_TYPE;
	}

	public void setEVENT_TYPE(String eVENT_TYPE) {
		EVENT_TYPE = eVENT_TYPE;
	}

	public String getDURATION() {
		return DURATION;
	}

	public void setDURATION(String dURATION) {
		DURATION = dURATION;
	}

}
