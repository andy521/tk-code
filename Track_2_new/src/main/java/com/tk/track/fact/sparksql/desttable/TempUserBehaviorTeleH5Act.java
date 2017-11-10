package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

public class TempUserBehaviorTeleH5Act  implements Serializable {

	private static final long serialVersionUID = -8272673935687867408L;
	
	private String ROWKEY;         //uuid 自动生成
	private String USER_ID;        //用于用户身份的标识
	private String APP_TYPE;
	private String APP_ID;      
	private String USER_EVENT;     //用户行为,对应浏览页面
	private String USER_SUBTYPE;   //用户行为,对应浏览页面
	private String PRODUCT_ID;     //产品ID
	private String CLASS_ID;       //类别ID
	private String CLASS_NAME;     //类别名称
	private String VISIT_COUNT;    //浏览次数
	private String VISIT_TIME;     //当日第一次浏览时间
	private String VISIT_DURATION; //浏览页面的总时长
	private String CLASSIFYNAME;   //产品分类
	private String PRODUCTNAME;    //产品名称
    private String USER_TYPE;      //用户类型
    private String LRT_ID;         //险种编码
    private String FROM_ID;        //渠道来源
    
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
	public String getUSER_EVENT() {
		return USER_EVENT;
	}
	public void setUSER_EVENT(String uSER_EVENT) {
		USER_EVENT = uSER_EVENT;
	}
	public String getUSER_SUBTYPE() {
		return USER_SUBTYPE;
	}
	public void setUSER_SUBTYPE(String uSER_SUBTYPE) {
		USER_SUBTYPE = uSER_SUBTYPE;
	}
	public String getPRODUCT_ID() {
		return PRODUCT_ID;
	}
	public void setPRODUCT_ID(String pRODUCT_ID) {
		PRODUCT_ID = pRODUCT_ID;
	}
	public String getCLASS_ID() {
		return CLASS_ID;
	}
	public void setCLASS_ID(String cLASS_ID) {
		CLASS_ID = cLASS_ID;
	}
	public String getCLASS_NAME() {
		return CLASS_NAME;
	}
	public void setCLASS_NAME(String cLASS_NAME) {
		CLASS_NAME = cLASS_NAME;
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
	public String getCLASSIFYNAME() {
		return CLASSIFYNAME;
	}
	public void setCLASSIFYNAME(String cLASSIFYNAME) {
		CLASSIFYNAME = cLASSIFYNAME;
	}
	public String getPRODUCTNAME() {
		return PRODUCTNAME;
	}
	public void setPRODUCTNAME(String pRODUCTNAME) {
		PRODUCTNAME = pRODUCTNAME;
	}
	
	public String getUSER_TYPE() {
		return USER_TYPE;
	}
	public void setUSER_TYPE(String uSER_TYPE) {
		USER_TYPE = uSER_TYPE;
	}
	public String getLRT_ID() {
		return LRT_ID;
	}
	public void setLRT_ID(String lRT_ID) {
		LRT_ID = lRT_ID;
	}
	public String getFROM_ID() {
		return FROM_ID;
	}
	public void setFROM_ID(String fROM_ID) {
		FROM_ID = fROM_ID;
	}
	
	
	public TempUserBehaviorTeleH5Act() {}
	
	public TempUserBehaviorTeleH5Act(String rOWKEY, String uSER_ID,
			String aPP_TYPE, String aPP_ID, String uSER_EVENT,
			String uSER_SUBTYPE, String pRODUCT_ID, String cLASS_ID,
			String cLASS_NAME, String vISIT_COUNT, String vISIT_TIME,
			String vISIT_DURATION, String cLASSIFYNAME, String pRODUCTNAME,
			String uSER_TYPE, String lRT_ID, String fROM_ID) {
		super();
		ROWKEY = rOWKEY;
		USER_ID = uSER_ID;
		APP_TYPE = aPP_TYPE;
		APP_ID = aPP_ID;
		USER_EVENT = uSER_EVENT;
		USER_SUBTYPE = uSER_SUBTYPE;
		PRODUCT_ID = pRODUCT_ID;
		CLASS_ID = cLASS_ID;
		CLASS_NAME = cLASS_NAME;
		VISIT_COUNT = vISIT_COUNT;
		VISIT_TIME = vISIT_TIME;
		VISIT_DURATION = vISIT_DURATION;
		CLASSIFYNAME = cLASSIFYNAME;
		PRODUCTNAME = pRODUCTNAME;
		USER_TYPE = uSER_TYPE;
		LRT_ID = lRT_ID;
		FROM_ID = fROM_ID;
	}
	
}
