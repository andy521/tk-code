package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

/**
 * @ClassName: FactNetTeleCallSkill
 * @Description: TODO
 * @author liran
 * @date   2016年8月1日
 */
public class FactNetTeleCallSkill implements Serializable {
    
    private static final long serialVersionUID = -3635456436620197466L;

    private String ROWKEY;         //uuid 自动生成
    private String APP_ID;         //APP类型
    private String USER_ID;        //用于用户身份的标识
    private String USER_EVENT;     //用户行为,对应浏览页面
	private String FROM_PAGE;      //开源页面
	private String CALLSKILL_ID;   //话术ID
	private String CALLSKILL_TYPE; //话术类型
	private String CREATED_USERID; //话术创建ID
	private String VISIT_COUNT;    //浏览次数
	private String VISIT_TIME;     //当日第一次浏览时间
	private String VISIT_DURATION; //浏览页面的总时长
	private String FIRST_LEVEL;    //一级分类
	private String SECOND_LEVEL;   //二级分类
	private String THIRD_LEVEL;    //三级分类
	private String FOURTH_LEVEL;   //四级分类
    
	public FactNetTeleCallSkill() {}

	public FactNetTeleCallSkill(String rOWKEY, String aPP_ID, String uSER_ID,
			String uSER_EVENT, String fROM_PAGE, String cALLSKILL_ID,
			String cALLSKILL_TYPE, String cREATED_USERID, String vISIT_COUNT,
			String vISIT_TIME, String vISIT_DURATION, String fIRST_LEVEL,
			String sECOND_LEVEL, String tHIRD_LEVEL, String fOURTH_LEVEL) {
		super();
		ROWKEY = rOWKEY;
		APP_ID = aPP_ID;
		USER_ID = uSER_ID;
		USER_EVENT = uSER_EVENT;
		FROM_PAGE = fROM_PAGE;
		CALLSKILL_ID = cALLSKILL_ID;
		CALLSKILL_TYPE = cALLSKILL_TYPE;
		CREATED_USERID = cREATED_USERID;
		VISIT_COUNT = vISIT_COUNT;
		VISIT_TIME = vISIT_TIME;
		VISIT_DURATION = vISIT_DURATION;
		FIRST_LEVEL = fIRST_LEVEL;
		SECOND_LEVEL = sECOND_LEVEL;
		THIRD_LEVEL = tHIRD_LEVEL;
		FOURTH_LEVEL = fOURTH_LEVEL;
	}

	public String getROWKEY() {
		return ROWKEY;
	}

	public void setROWKEY(String rOWKEY) {
		ROWKEY = rOWKEY;
	}

	public String getAPP_ID() {
		return APP_ID;
	}

	public void setAPP_ID(String aPP_ID) {
		APP_ID = aPP_ID;
	}

	public String getUSER_ID() {
		return USER_ID;
	}

	public void setUSER_ID(String uSER_ID) {
		USER_ID = uSER_ID;
	}

	public String getUSER_EVENT() {
		return USER_EVENT;
	}

	public void setUSER_EVENT(String uSER_EVENT) {
		USER_EVENT = uSER_EVENT;
	}

	public String getFROM_PAGE() {
		return FROM_PAGE;
	}

	public void setFROM_PAGE(String fROM_PAGE) {
		FROM_PAGE = fROM_PAGE;
	}

	public String getCALLSKILL_ID() {
		return CALLSKILL_ID;
	}

	public void setCALLSKILL_ID(String cALLSKILL_ID) {
		CALLSKILL_ID = cALLSKILL_ID;
	}

	public String getCALLSKILL_TYPE() {
		return CALLSKILL_TYPE;
	}

	public void setCALLSKILL_TYPE(String cALLSKILL_TYPE) {
		CALLSKILL_TYPE = cALLSKILL_TYPE;
	}

	public String getCREATED_USERID() {
		return CREATED_USERID;
	}

	public void setCREATED_USERID(String cREATED_USERID) {
		CREATED_USERID = cREATED_USERID;
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

	public String getFIRST_LEVEL() {
		return FIRST_LEVEL;
	}

	public void setFIRST_LEVEL(String fIRST_LEVEL) {
		FIRST_LEVEL = fIRST_LEVEL;
	}

	public String getSECOND_LEVEL() {
		return SECOND_LEVEL;
	}

	public void setSECOND_LEVEL(String sECOND_LEVEL) {
		SECOND_LEVEL = sECOND_LEVEL;
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
	

}
