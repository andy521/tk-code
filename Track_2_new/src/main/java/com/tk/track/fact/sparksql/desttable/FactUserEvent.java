package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

public class FactUserEvent  implements Serializable {

	private static final long serialVersionUID = 117112193402297112L;

	private String ROWKEY;
	private String USER_ID;
	private String APP_TYPE;
	private String APP_ID;
	private String PAGE;
	private String EVENT;
	private String SUBTYPE;
	private String LABEL;
	private String SYSINFO;
	private String TERMINAL_ID;
	private String TIME;
	private String FROM_ID;
	
	
	public FactUserEvent(String rOWKEY, String uSER_ID, String aPP_TYPE, String aPP_ID, String pAGE, String eVENT,
			String sUBTYPE, String lABEL, String sYSINFO, String tERMINAL_ID, String tIME,String fROM_ID) {
		super();
		ROWKEY = rOWKEY;
		USER_ID = uSER_ID;
		APP_TYPE = aPP_TYPE;
		APP_ID = aPP_ID;
		PAGE = pAGE;
		EVENT = eVENT;
		SUBTYPE = sUBTYPE;
		LABEL = lABEL;
		SYSINFO = sYSINFO;
		TERMINAL_ID = tERMINAL_ID;
		TIME = tIME;
		this.FROM_ID = fROM_ID;
	}


	@Override
	public String toString() {
		return "FactUserEvent [ROWKEY=" + ROWKEY + ", USER_ID=" + USER_ID + ", APP_TYPE=" + APP_TYPE + ", APP_ID="
				+ APP_ID + ", PAGE=" + PAGE + ", EVENT=" + EVENT + ", SUBTYPE=" + SUBTYPE + ", LABEL=" + LABEL
				+ ", SYSINFO=" + SYSINFO + ", TERMINAL_ID=" + TERMINAL_ID + ", TIME=" + TIME + ",FROM_ID=" + FROM_ID + "]";
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


	public void setUSER_ID(String uSER_ID) {
		USER_ID = uSER_ID;
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


	public String getPAGE() {
		return PAGE;
	}


	public void setPAGE(String pAGE) {
		PAGE = pAGE;
	}


	public String getEVENT() {
		return EVENT;
	}


	public void setEVENT(String eVENT) {
		EVENT = eVENT;
	}


	public String getSUBTYPE() {
		return SUBTYPE;
	}


	public void setSUBTYPE(String sUBTYPE) {
		SUBTYPE = sUBTYPE;
	}


	public String getLABEL() {
		return LABEL;
	}


	public void setLABEL(String lABEL) {
		LABEL = lABEL;
	}


	public String getSYSINFO() {
		return SYSINFO;
	}


	public void setSYSINFO(String sYSINFO) {
		SYSINFO = sYSINFO;
	}


	public String getTERMINAL_ID() {
		return TERMINAL_ID;
	}


	public void setTERMINAL_ID(String tERMINAL_ID) {
		TERMINAL_ID = tERMINAL_ID;
	}


	public String getTIME() {
		return TIME;
	}


	public void setTIME(String tIME) {
		TIME = tIME;
	}


	public String getFROM_ID() {
		return FROM_ID;
	}


	public void setFROM_ID(String fROM_ID) {
		FROM_ID = fROM_ID;
	}


	public FactUserEvent() {}
	
}
