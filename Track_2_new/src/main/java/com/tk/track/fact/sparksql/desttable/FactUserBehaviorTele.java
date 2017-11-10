/**
 * @Title: FactUserBehaviorTele.java
 * @Package: com.tk.track.fact.sparksql.desttable
 * @Description: TODO
 * @author zhang.shy
 * @Data 2016年5月6日
 * @version 1.0
 */
package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

/**
 * @ClassName: FactUserBehaviorTele
 * @Description: TODO
 * @author zhang.shy
 * @date   2016年5月6日 
 */
public class FactUserBehaviorTele implements Serializable {

	private static final long serialVersionUID = -3440459877664397493L;
	
	private String ROWKEY;         //uuid 自动生成
    private String USER_ID;        //用于用户身份的标识
    private String USER_TYPE;      //客户类型 M:手机;C:客户号;MEM:会员号；WE:微信
    private String USER_EVENT;     //用户行为,对应浏览页面
    private String THIRD_LEVEL;    //三级分类
    private String FOURTH_LEVEL;   //四级分类
    private String LRT_ID;         //商品ID
    private String CLASS_ID;       //类别ID
    private String INFOFROM;       //信息来源渠道
    private String VISIT_COUNT;    //浏览次数
    private String VISIT_TIME;     //当日第一次浏览时间
    private String VISIT_DURATION; //浏览页面的总时长
    private String CLASSIFY_NAME;
    private String PRODUCT_NAME;
    private String USER_NAME;
    private String IF_PAY;
    private String FROM_ID;//渠道来源
    private String REMARK;//备注
    private String CLUE_TYPE;//线索类型
 

	public FactUserBehaviorTele(String rOWKEY, String uSER_ID, String uSER_TYPE, String uSER_EVENT, String tHIRD_LEVEL,
			String fOURTH_LEVEL, String lRT_ID, String cLASS_ID, String iNFOFROM, String vISIT_COUNT, String vISIT_TIME,
			String vISIT_DURATION, String cLASSIFY_NAME, String pRODUCT_NAME, String uSER_NAME, String iF_PAY,
			String fromId,String remark,String CLUE_TYPE) {
		super();
		ROWKEY = rOWKEY;
		USER_ID = uSER_ID;
		USER_TYPE = uSER_TYPE;
		USER_EVENT = uSER_EVENT;
		THIRD_LEVEL = tHIRD_LEVEL;
		FOURTH_LEVEL = fOURTH_LEVEL;
		LRT_ID = lRT_ID;
		CLASS_ID = cLASS_ID;
		INFOFROM = iNFOFROM;
		VISIT_COUNT = vISIT_COUNT;
		VISIT_TIME = vISIT_TIME;
		VISIT_DURATION = vISIT_DURATION;
		CLASSIFY_NAME = cLASSIFY_NAME;
		PRODUCT_NAME = pRODUCT_NAME;
		USER_NAME = uSER_NAME;
		IF_PAY = iF_PAY;
		this.FROM_ID = fromId;
		this.REMARK = remark;
		this.CLUE_TYPE = CLUE_TYPE;
	}


	@Override
	public String toString() {
		return "FactUserBehaviorTele [ROWKEY=" + ROWKEY + ", USER_ID=" + USER_ID + ", USER_TYPE=" + USER_TYPE
				+ ", USER_EVENT=" + USER_EVENT + ", THIRD_LEVEL=" + THIRD_LEVEL + ", FOURTH_LEVEL=" + FOURTH_LEVEL
				+ ", LRT_ID=" + LRT_ID + ", CLASS_ID=" + CLASS_ID + ", INFOFROM=" + INFOFROM + ", VISIT_COUNT="
				+ VISIT_COUNT + ", VISIT_TIME=" + VISIT_TIME + ", VISIT_DURATION=" + VISIT_DURATION + ", CLASSIFY_NAME="
				+ CLASSIFY_NAME + ", PRODUCT_NAME=" + PRODUCT_NAME + ", USER_NAME=" + USER_NAME + ", IF_PAY=" + IF_PAY
				+ ",FROM_ID=" + FROM_ID + ",REMARK=" + REMARK + ",CLUE_TYPE=" + CLUE_TYPE + "]";
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


	public String getUSER_EVENT() {
		return USER_EVENT;
	}


	public void setUSER_EVENT(String uSER_EVENT) {
		USER_EVENT = uSER_EVENT;
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


	public String getCLASSIFY_NAME() {
		return CLASSIFY_NAME;
	}


	public void setCLASSIFY_NAME(String cLASSIFY_NAME) {
		CLASSIFY_NAME = cLASSIFY_NAME;
	}


	public String getPRODUCT_NAME() {
		return PRODUCT_NAME;
	}


	public void setPRODUCT_NAME(String pRODUCT_NAME) {
		PRODUCT_NAME = pRODUCT_NAME;
	}


	public String getUSER_NAME() {
		return USER_NAME;
	}


	public void setUSER_NAME(String uSER_NAME) {
		USER_NAME = uSER_NAME;
	}

	public String getIF_PAY() {
		return IF_PAY;
	}


	public void setIF_PAY(String iF_PAY) {
		IF_PAY = iF_PAY;
	}

	

	public String getFROM_ID() {
		return FROM_ID;
	}


	public void setFROM_ID(String fROM_ID) {
		FROM_ID = fROM_ID;
	}


	public String getREMARK() {
		return REMARK;
	}


	public void setREMARK(String rEMARK) {
		REMARK = rEMARK;
	}



	public String getCLUE_TYPE() {
		return CLUE_TYPE;
	}


	public void setCLUE_TYPE(String cLUE_TYPE) {
		CLUE_TYPE = cLUE_TYPE;
	}


	public FactUserBehaviorTele() {
    }

}
