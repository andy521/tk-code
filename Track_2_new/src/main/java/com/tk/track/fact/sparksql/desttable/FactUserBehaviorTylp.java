package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

public class FactUserBehaviorTylp implements  Serializable{
    
	private static final long serialVersionUID = 1L;
	 
	  private String ROWKEY;
      private String USER_ID;
      private String USER_NAME;
      private String BIRTH;
      private String GENDER;
      private String USER_TYPE;
      private String USER_EVENT;
      private String THIRD_LEVEL;
      private String FOURTH_LEVEL;
      private String INFOFROM;
      private String VISIT_COUNT;
      private String FROM_ID;
      private String VISIT_TIME;
      private String LONGEST_PATH;
	
      
      public FactUserBehaviorTylp() {
		super();
	}


	

  
	
	
	
	
	public FactUserBehaviorTylp(String rOWKEY, String uSER_ID, String uSER_NAME, String bIRTH, String gENDER,
			String uSER_TYPE, String uSER_EVENT, String tHIRD_LEVEL, String fOURTH_LEVEL, String iNFOFROM,
			String vISIT_COUNT, String fROM_ID, String vISIT_TIME, String lONGEST_PATH) {
		super();
		ROWKEY = rOWKEY;
		USER_ID = uSER_ID;
		USER_NAME = uSER_NAME;
		BIRTH = bIRTH;
		GENDER = gENDER;
		USER_TYPE = uSER_TYPE;
		USER_EVENT = uSER_EVENT;
		THIRD_LEVEL = tHIRD_LEVEL;
		FOURTH_LEVEL = fOURTH_LEVEL;
		INFOFROM = iNFOFROM;
		VISIT_COUNT = vISIT_COUNT;
		FROM_ID = fROM_ID;
		VISIT_TIME = vISIT_TIME;
		LONGEST_PATH = lONGEST_PATH;
	}









	public String getTHIRD_LEVEL() {
		return THIRD_LEVEL;
	}


	public void setTHIRD_LEVEL(String tHIRD_LEVEL) {
		THIRD_LEVEL = tHIRD_LEVEL;
	}


	public String getFOURTH_LEVEL() {
		return FOURTH_LEVEL;
	}


	public void setFOURTH_LEVEL(String fOURTH_LEVEL) {
		FOURTH_LEVEL = fOURTH_LEVEL;
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


	public String getUSER_NAME() {
		return USER_NAME;
	}


	public void setUSER_NAME(String uSER_NAME) {
		USER_NAME = uSER_NAME;
	}


	public String getBIRTH() {
		return BIRTH;
	}


	public void setBIRTH(String bIRTH) {
		BIRTH = bIRTH;
	}


	public String getGENDER() {
		return GENDER;
	}


	public void setGENDER(String gENDER) {
		GENDER = gENDER;
	}


	public String getUSER_TYPE() {
		return USER_TYPE;
	}


	public void setUSER_TYPE(String uSER_TYPE) {
		USER_TYPE = uSER_TYPE;
	}


	public String getUSER_EVENT() {
		return USER_EVENT;
	}


	public void setUSER_EVENT(String uSER_EVENT) {
		USER_EVENT = uSER_EVENT;
	}


	


	public String getINFOFROM() {
		return INFOFROM;
	}


	public void setINFOFROM(String iNFOFROM) {
		INFOFROM = iNFOFROM;
	}


	

	public String getVISIT_COUNT() {
		return VISIT_COUNT;
	}


	public void setVISIT_COUNT(String vISIT_COUNT) {
		VISIT_COUNT = vISIT_COUNT;
	}


	public String getFROM_ID() {
		return FROM_ID;
	}


	public void setFROM_ID(String fROM_ID) {
		FROM_ID = fROM_ID;
	}


	public String getVISIT_TIME() {
		return VISIT_TIME;
	}


	public void setVISIT_TIME(String vISIT_TIME) {
		VISIT_TIME = vISIT_TIME;
	}


	public String getLONGEST_PATH() {
		return LONGEST_PATH;
	}


	public void setLONGEST_PATH(String lONGEST_PATH) {
		LONGEST_PATH = lONGEST_PATH;
	}


	public static long getSerialversionuid() {
		return serialVersionUID;
	}
      
      
      
}
