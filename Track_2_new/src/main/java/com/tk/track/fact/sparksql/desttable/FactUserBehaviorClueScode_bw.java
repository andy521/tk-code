package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

/**
 * @ClassName: FactUserBehaviorClueScode
 * @Description: 用户行为线索scode解析
 * @author 
 * @date   
 */
public class FactUserBehaviorClueScode_bw implements Serializable {

	private static final long serialVersionUID = -3440459877664397493L;
	
	private String ROWKEY;         //uuid 自动生成
    private String USER_ID;        //用于用户身份的标识
    private String USER_TYPE;      //客户类型 M:手机;C:客户号;MEM:会员号；WE:微信
    private String APP_TYPE;       //插码时分配的
    private String APP_ID;         //插码时分配的
    private String EVENT_TYPE;     //行为类型（load/click/blur）
    private String EVENT;     	   //用户行为,对应浏览页面
    private String SUB_TYPE;       //用户行为,对应浏览页面
    private String VISIT_DURATION; //浏览页面的总时长
    private String FROM_ID;        //渠道来源
    private String PAGE_TYPE;      //页面分类（查询/投保/理赔/资讯/服务/活动）
    private String FIRST_LEVEL;    //一级线索分类
    private String SECOND_LEVEL;   //二级线索分类
    private String THIRD_LEVEL;    //三级分类
    private String FOURTH_LEVEL;   //四级分类
    private String VISIT_TIME;     //当日第一次浏览时间
    private String VISIT_COUNT;     //当日第一次浏览时间
    private String CLUE_TYPE;      //线索/事件/其他
    private String REMARK;		   //备注

	public FactUserBehaviorClueScode_bw(String ROWKEY, String USER_ID,
			String USER_TYPE, String APP_TYPE, String APP_ID,
			String EVENT, String EVENT_TYPE, String SUB_TYPE,
			String VISIT_COUNT, String VISIT_TIME, String VISIT_DURATION,
			String FROM_ID, String PAGE_TYPE, String FIRST_LEVEL,
			String SECOND_LEVEL, String THIRD_LEVEL, String FOURTH_LEVEL,
			String CLUE_TYPE, String REMARK ) {
		this.ROWKEY = ROWKEY;
		this.USER_ID = USER_ID;
		this.USER_TYPE = USER_TYPE;
		this.APP_TYPE = APP_TYPE;
		this.APP_ID = APP_ID;
		this.EVENT_TYPE = EVENT_TYPE;
		this.EVENT = EVENT;
		this.SUB_TYPE = SUB_TYPE;
		this.VISIT_DURATION = VISIT_DURATION;
		this.FROM_ID = FROM_ID;
		this.PAGE_TYPE = PAGE_TYPE;
		this.FIRST_LEVEL = FIRST_LEVEL;
		this.SECOND_LEVEL = SECOND_LEVEL;
		this.THIRD_LEVEL = THIRD_LEVEL;
		this.FOURTH_LEVEL = FOURTH_LEVEL;
		this.VISIT_TIME = VISIT_TIME;
		this.VISIT_COUNT = VISIT_COUNT;
		this.CLUE_TYPE = CLUE_TYPE;
		this.REMARK = REMARK;

	}

	@Override
	public String toString() {
		return "FactUserBehaviorClue [ROWKEY=" + ROWKEY + ", USER_ID=" + USER_ID + ", USER_TYPE=" + USER_TYPE
				+ ", APP_TYPE=" + APP_TYPE + ", APP_ID=" + APP_ID + ", EVENT_TYPE=" + EVENT_TYPE
				+ ", EVENT=" + EVENT + ", SUB_TYPE=" + SUB_TYPE + ", VISIT_DURATION=" + VISIT_DURATION + ", FROM_ID="
				+ FROM_ID + ", PAGE_TYPE=" + PAGE_TYPE + ", FIRST_LEVEL=" + FIRST_LEVEL + ", SECOND_LEVEL="
				+ SECOND_LEVEL + ", THIRD_LEVEL=" + THIRD_LEVEL + ", FOURTH_LEVEL=" + FOURTH_LEVEL + ", VISIT_TIME=" + VISIT_TIME
				+ ",VISIT_COUNT=" + VISIT_COUNT + ",CLUE_TYPE=" + CLUE_TYPE + ",REMARK=" + REMARK + ",USER_NAME=\" + USER_NAME + \"]";
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

	public String getUSER_TYPE() {
		return USER_TYPE;
	}

	public void setUSER_TYPE(String uSER_TYPE) {
		USER_TYPE = uSER_TYPE;
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

	public String getEVENT_TYPE() {
		return EVENT_TYPE;
	}

	public void setEVENT_TYPE(String eVENT_TYPE) {
		EVENT_TYPE = eVENT_TYPE;
	}

	public String getEVENT() {
		return EVENT;
	}

	public void setEVENT(String eVENT) {
		EVENT = eVENT;
	}

	public String getSUB_TYPE() {
		return SUB_TYPE;
	}

	public void setSUB_TYPE(String sUB_TYPE) {
		SUB_TYPE = sUB_TYPE;
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

	public String getPAGE_TYPE() {
		return PAGE_TYPE;
	}

	public void setPAGE_TYPE(String pAGE_TYPE) {
		PAGE_TYPE = pAGE_TYPE;
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

	public String getVISIT_TIME() {
		return VISIT_TIME;
	}

	public void setVISIT_TIME(String vISIT_TIME) {
		VISIT_TIME = vISIT_TIME;
	}

	public String getCLUE_TYPE() {
		return CLUE_TYPE;
	}

	public void setCLUE_TYPE(String cLUE_TYPE) {
		CLUE_TYPE = cLUE_TYPE;
	}

	public String getREMARK() {
		return REMARK;
	}

	public void setREMARK(String rEMARK) {
		REMARK = rEMARK;
	}

	public String getVISIT_COUNT() {
		return VISIT_COUNT;
	}

	public void setVISIT_COUNT(String vISIT_COUNT) {
		VISIT_COUNT = vISIT_COUNT;
	}

	
}
