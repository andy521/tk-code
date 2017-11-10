package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

public class FactIntegeralMall implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4163392029262877287L;

	private String OPEN_ID;    
    private String APP_TYPE;
    private String APP_ID;      
    private String EVENT;    
    private String SUBTYPE;
    private String VISIT_COUNT;    
    private String VISIT_TIME;     
    private String VISIT_DURATION;     
    private String FROM_ID;        
    
    private String PRODUCT_ID;
    private String PRODUCT_NAME;
    private String CLASSIFY;
    private String TRADE_NUM;
    private String RESULT;


	public FactIntegeralMall(String oPEN_ID, String aPP_TYPE, String aPP_ID, String eVENT, String sUBTYPE,
			String vISIT_COUNT, String vISIT_TIME, String vISIT_DURATION, String fROM_ID, String pRODUCT_ID,
			String pRODUCT_NAME, String cLASSIFY, String tRADE_NUM, String rESULT) {
		super();
		OPEN_ID = oPEN_ID;
		APP_TYPE = aPP_TYPE;
		APP_ID = aPP_ID;
		EVENT = eVENT;
		SUBTYPE = sUBTYPE;
		VISIT_COUNT = vISIT_COUNT;
		VISIT_TIME = vISIT_TIME;
		VISIT_DURATION = vISIT_DURATION;
		FROM_ID = fROM_ID;
		PRODUCT_ID = pRODUCT_ID;
		PRODUCT_NAME = pRODUCT_NAME;
		CLASSIFY = cLASSIFY;
		TRADE_NUM = tRADE_NUM;
		RESULT = rESULT;
	}



	public FactIntegeralMall() {
		// TODO Auto-generated constructor stub
	}



	public String getOPEN_ID() {
		return OPEN_ID;
	}



	public void setOPEN_ID(String oPEN_ID) {
		OPEN_ID = oPEN_ID;
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



	public String getVISIT_COUNT() {
		return VISIT_COUNT;
	}



	public void setVISIT_COUNT(String vISIT_COUNT) {
		VISIT_COUNT = vISIT_COUNT;
	}



	public String getVISIT_TIME() {
		return VISIT_TIME;
	}



	public void setVISIT_TIME(String vISIT_TIME) {
		VISIT_TIME = vISIT_TIME;
	}



	public String getVISIT_DURATION() {
		return VISIT_DURATION;
	}



	public void setVISIT_DURATION(String vISIT_DURATION) {
		VISIT_DURATION = vISIT_DURATION;
	}



	public String getFROM_ID() {
		return FROM_ID;
	}



	public void setFROM_ID(String fROM_ID) {
		FROM_ID = fROM_ID;
	}



	public String getPRODUCT_ID() {
		return PRODUCT_ID;
	}



	public void setPRODUCT_ID(String pRODUCT_ID) {
		PRODUCT_ID = pRODUCT_ID;
	}



	public String getPRODUCT_NAME() {
		return PRODUCT_NAME;
	}



	public void setPRODUCT_NAME(String pRODUCT_NAME) {
		PRODUCT_NAME = pRODUCT_NAME;
	}



	public String getCLASSIFY() {
		return CLASSIFY;
	}



	public void setCLASSIFY(String cLASSIFY) {
		CLASSIFY = cLASSIFY;
	}



	public String getTRADE_NUM() {
		return TRADE_NUM;
	}



	public void setTRADE_NUM(String tRADE_NUM) {
		TRADE_NUM = tRADE_NUM;
	}



	public String getRESULT() {
		return RESULT;
	}



	public void setRESULT(String rESULT) {
		RESULT = rESULT;
	}



	

}
