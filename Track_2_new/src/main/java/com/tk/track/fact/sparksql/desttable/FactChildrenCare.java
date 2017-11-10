package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;


public class FactChildrenCare implements Serializable {

	/**
	 * 
	 */

	private static final long serialVersionUID = 1L;
	
	private String OPEN_ID;    
    private String APP_TYPE;
    private String APP_ID;      
    private String EVENT;      
    

    private String PRODUCT_NAME;


	public FactChildrenCare() {
		super();
	}


	public FactChildrenCare(String oPEN_ID, String aPP_TYPE, String aPP_ID, String eVENT,String pRODUCT_NAME) {
		super();
		OPEN_ID = oPEN_ID;
		APP_TYPE = aPP_TYPE;
		APP_ID = aPP_ID;
		EVENT = eVENT;
		PRODUCT_NAME = pRODUCT_NAME;
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


	public String getPRODUCT_NAME() {
		return PRODUCT_NAME;
	}


	public void setPRODUCT_NAME(String pRODUCT_NAME) {
		PRODUCT_NAME = pRODUCT_NAME;
	}
    
    
	
}
