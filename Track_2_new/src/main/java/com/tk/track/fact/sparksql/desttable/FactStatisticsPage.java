package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

public class FactStatisticsPage implements Serializable {

	private static final long serialVersionUID = 7249963364094505430L;
	
	private String ROWKEY;
	private String APP_TYPE;
	private String APP_ID;
	private String PAGE;
	private String REFER;
	private String CURRENTURL;
	private String TIME;
	private String DURATION;
	private String FROM_ID;


	@Override
	public String toString() {
		return "FactStatisticsPage [ROWKEY=" + ROWKEY + ", APP_TYPE=" + APP_TYPE + ", APP_ID=" + APP_ID + ", PAGE="
				+ PAGE + ", REFER=" + REFER + ", CURRENTURL=" + CURRENTURL + ", TIME=" + TIME + ", DURATION=" + DURATION
				+ "FROM_ID=" + FROM_ID + "]";
	}


	public FactStatisticsPage(String rOWKEY, String aPP_TYPE, String aPP_ID, String pAGE, String rEFER,
			String cURRENTURL, String tIME, String dURATION,String fROM_ID) {
		super();
		ROWKEY = rOWKEY;
		APP_TYPE = aPP_TYPE;
		APP_ID = aPP_ID;
		PAGE = pAGE;
		REFER = rEFER;
		CURRENTURL = cURRENTURL;
		TIME = tIME;
		DURATION = dURATION;
		this.FROM_ID = fROM_ID;
	}


	public String getROWKEY() {
		return ROWKEY;
	}


	public void setROWKEY(String rOWKEY) {
		ROWKEY = rOWKEY;
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


	public String getREFER() {
		return REFER;
	}


	public void setREFER(String rEFER) {
		REFER = rEFER;
	}


	public String getCURRENTURL() {
		return CURRENTURL;
	}


	public void setCURRENTURL(String cURRENTURL) {
		CURRENTURL = cURRENTURL;
	}


	public String getTIME() {
		return TIME;
	}


	public void setTIME(String tIME) {
		TIME = tIME;
	}


	public String getDURATION() {
		return DURATION;
	}


	public void setDURATION(String dURATION) {
		DURATION = dURATION;
	}

	
	public String getFROM_ID() {
		return FROM_ID;
	}


	public void setFROM_ID(String fROM_ID) {
		FROM_ID = fROM_ID;
	}


	public FactStatisticsPage() {}
}
