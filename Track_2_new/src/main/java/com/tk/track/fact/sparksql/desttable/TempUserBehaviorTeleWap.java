package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;
/**
 * 
 * @author itw_liran
 * 与Wechat共用
 *
 */
public class TempUserBehaviorTeleWap  implements Serializable {

	private static final long serialVersionUID = 3827927072996950833L;
	
	private String ROWKEY;         //uuid 自动生成
	private String USER_ID;        //用于用户身份的标识
    private String APP_TYPE;
    private String APP_ID;      
    private String USER_EVENT;     //用户行为,对应浏览页面
    private String USER_SUBTYPE;   //用户行为,对应浏览页面
    private String LRT_ID;         //LRT_ID
    private String CLASS_ID;       //类别ID
    //public String CLASS_NAME;     //类别名称
    private String VISIT_COUNT;    //浏览次数
    private String VISIT_TIME;     //当日第一次浏览时间
    private String VISIT_DURATION; //浏览页面的总时长
    private String CLASSIFYNAME;   //产品分类
    private String LRT_NAME;       //LRT名称
    private String THIRD_LEVEL;    //三级分类
    private String FOURTH_LEVEL;   //四级分类
    private String USER_TYPE;      //用户类型
    private String FROM_ID;        //渠道来源
    
	public TempUserBehaviorTeleWap() {
	}
	
	public TempUserBehaviorTeleWap(String rOWKEY, String uSER_ID,
			String aPP_TYPE, String aPP_ID, String uSER_EVENT,
			String uSER_SUBTYPE, String lRT_ID, String cLASS_ID,
			String vISIT_COUNT, String vISIT_TIME, String vISIT_DURATION,
			String cLASSIFYNAME, String lRT_NAME, String tHIRD_LEVEL,
			String fOURTH_LEVEL, String uSER_TYPE, String fROM_ID) {
		super();
		ROWKEY = rOWKEY;
		USER_ID = uSER_ID;
		APP_TYPE = aPP_TYPE;
		APP_ID = aPP_ID;
		USER_EVENT = uSER_EVENT;
		USER_SUBTYPE = uSER_SUBTYPE;
		LRT_ID = lRT_ID;
		CLASS_ID = cLASS_ID;
		VISIT_COUNT = vISIT_COUNT;
		VISIT_TIME = vISIT_TIME;
		VISIT_DURATION = vISIT_DURATION;
		CLASSIFYNAME = cLASSIFYNAME;
		LRT_NAME = lRT_NAME;
		THIRD_LEVEL = tHIRD_LEVEL;
		FOURTH_LEVEL = fOURTH_LEVEL;
		USER_TYPE = uSER_TYPE;
		FROM_ID = fROM_ID;
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
	public String getLRT_ID() {
		return LRT_ID;
	}
	public void setLRT_ID(String lRT_ID) {
		LRT_ID = lRT_ID;
	}
	public String getCLASS_ID() {
		return CLASS_ID;
	}
	public void setCLASS_ID(String cLASS_ID) {
		CLASS_ID = cLASS_ID;
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
	public String getLRT_NAME() {
		return LRT_NAME;
	}
	public void setLRT_NAME(String lRT_NAME) {
		LRT_NAME = lRT_NAME;
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
	public String getUSER_TYPE() {
		return USER_TYPE;
	}
	public void setUSER_TYPE(String uSER_TYPE) {
		USER_TYPE = uSER_TYPE;
	}
	public String getFROM_ID() {
		return FROM_ID;
	}
	public void setFROM_ID(String fROM_ID) {
		FROM_ID = fROM_ID;
	}

}
