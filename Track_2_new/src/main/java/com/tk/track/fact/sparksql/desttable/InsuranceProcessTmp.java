package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

/**
 * 
* <p>Title: 泰康大数据部</p>
* <p>Description: 投保流程线索临时表</p>
* <p>Copyright: Copyright (c) 2016</p>
* <p>Company: 大连华信有限公司</p>
* <p>Department: 总公司\数据信息中心\IT外包公司\大连华信总公司\数据信息中心\IT外包公司\大连华信部</p>
* @author moyunqing
* @date   2016年9月22日
* @version 1.0
 */
public class InsuranceProcessTmp implements Serializable{
	private static final long serialVersionUID = -2173082015051019259L;
	private String APP_TYPE;
	private String APP_ID;
	private String EVENT;
	private String SUBTYPE;
	private String TERMINAL_ID;
	private String USER_ID;
	private String VISIT_TIME;
	private String FROM_ID;
	private String FORM_ID;
	private String LRT_ID;
	private String LRT_NAME;
	private String SALE_MODE;
	private String BABY_GENDER;
	private String BABY_BIRTHDAY;
	private String INSURE_TYPE;
	private String SEX;
	private String BABY_RELATION;//是宝贝的什么
	private String P_BIRTHDAY;//宝贝父母生日
	private String PAY_TYPE;//交费方式
	private String PAY_PERIOD;//交费频率
	private String PH_NAME;
	private String PH_BIRTHDAY;
	private String PH_EMAIL;
	private String PH_INDUSTRY;
	private String PH_WORKTYPE;
	private String INS_RELATION;
	private String INS_NAME;
	private String INS_BIRTHDAY;
	private String BENE_RELATION;
	private String BENE_NAME;
	private String BENE_BIRTHDAY;
	private String SUCCESS;
	
	
	
	public InsuranceProcessTmp(String aPP_TYPE, String aPP_ID, String eVENT, String sUBTYPE, String tERMINAL_ID,
			String uSER_ID, String vISIT_TIME, String fROM_ID, String fORM_ID, String lRT_ID, String lRT_NAME,
			String sALE_MODE, String bABY_GENDER, String bABY_BIRTHDAY, String iNSURE_TYPE, String bIRTHDAY, String sEX,
			String bABY_RELATION, String p_BIRTHDAY, String pAY_TYPE, String pAY_PERIOD, String pH_NAME,
			String pH_BIRTHDAY, String pH_EMAIL, String pH_INDUSTRY, String pH_WORKTYPE, String iNS_RELATION,
			String iNS_NAME, String iNS_BIRTHDAY, String bENE_RELATION, String bENE_NAME, String bENE_BIRTHDAY,
			String sUCCESS) {
		super();
		APP_TYPE = aPP_TYPE;
		APP_ID = aPP_ID;
		EVENT = eVENT;
		SUBTYPE = sUBTYPE;
		TERMINAL_ID = tERMINAL_ID;
		USER_ID = uSER_ID;
		VISIT_TIME = vISIT_TIME;
		FROM_ID = fROM_ID;
		FORM_ID = fORM_ID;
		LRT_ID = lRT_ID;
		LRT_NAME = lRT_NAME;
		SALE_MODE = sALE_MODE;
		BABY_GENDER = bABY_GENDER;
		BABY_BIRTHDAY = bABY_BIRTHDAY;
		INSURE_TYPE = iNSURE_TYPE;
		SEX = sEX;
		BABY_RELATION = bABY_RELATION;
		P_BIRTHDAY = p_BIRTHDAY;
		PAY_TYPE = pAY_TYPE;
		PAY_PERIOD = pAY_PERIOD;
		PH_NAME = pH_NAME;
		PH_BIRTHDAY = pH_BIRTHDAY;
		PH_EMAIL = pH_EMAIL;
		PH_INDUSTRY = pH_INDUSTRY;
		PH_WORKTYPE = pH_WORKTYPE;
		INS_RELATION = iNS_RELATION;
		INS_NAME = iNS_NAME;
		INS_BIRTHDAY = iNS_BIRTHDAY;
		BENE_RELATION = bENE_RELATION;
		BENE_NAME = bENE_NAME;
		BENE_BIRTHDAY = bENE_BIRTHDAY;
		SUCCESS = sUCCESS;
	}

	public InsuranceProcessTmp(){}
	
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
	public String getTERMINAL_ID() {
		return TERMINAL_ID;
	}
	public void setTERMINAL_ID(String tERMINAL_ID) {
		TERMINAL_ID = tERMINAL_ID;
	}
	public String getUSER_ID() {
		return USER_ID;
	}
	public void setUSER_ID(String uSER_ID) {
		USER_ID = uSER_ID;
	}
	public String getVISIT_TIME() {
		return VISIT_TIME;
	}
	public void setVISIT_TIME(String vISIT_TIME) {
		VISIT_TIME = vISIT_TIME;
	}
	public String getFROM_ID() {
		return FROM_ID;
	}
	public void setFROM_ID(String fROM_ID) {
		FROM_ID = fROM_ID;
	}
	public String getFORM_ID() {
		return FORM_ID;
	}
	public void setFORM_ID(String fORM_ID) {
		FORM_ID = fORM_ID;
	}
	public String getLRT_ID() {
		return LRT_ID;
	}
	public void setLRT_ID(String lRT_ID) {
		LRT_ID = lRT_ID;
	}
	public String getLRT_NAME() {
		return LRT_NAME;
	}
	public void setLRT_NAME(String lRT_NAME) {
		LRT_NAME = lRT_NAME;
	}
	public String getSALE_MODE() {
		return SALE_MODE;
	}
	public void setSALE_MODE(String sALE_MODE) {
		SALE_MODE = sALE_MODE;
	}
	public String getBABY_GENDER() {
		return BABY_GENDER;
	}
	public void setBABY_GENDER(String bABY_GENDER) {
		BABY_GENDER = bABY_GENDER;
	}
	public String getBABY_BIRTHDAY() {
		return BABY_BIRTHDAY;
	}
	public void setBABY_BIRTHDAY(String bABY_BIRTHDAY) {
		BABY_BIRTHDAY = bABY_BIRTHDAY;
	}
	public String getINSURE_TYPE() {
		return INSURE_TYPE;
	}
	public void setINSURE_TYPE(String iNSURE_TYPE) {
		INSURE_TYPE = iNSURE_TYPE;
	}
	public String getSEX() {
		return SEX;
	}
	public void setSEX(String sEX) {
		SEX = sEX;
	}
	public String getBABY_RELATION() {
		return BABY_RELATION;
	}
	public void setBABY_RELATION(String bABY_RELATION) {
		BABY_RELATION = bABY_RELATION;
	}
	public String getP_BIRTHDAY() {
		return P_BIRTHDAY;
	}
	public void setP_BIRTHDAY(String p_BIRTHDAY) {
		P_BIRTHDAY = p_BIRTHDAY;
	}
	public String getPH_NAME() {
		return PH_NAME;
	}
	public void setPH_NAME(String pH_NAME) {
		PH_NAME = pH_NAME;
	}
	public String getPH_BIRTHDAY() {
		return PH_BIRTHDAY;
	}
	public void setPH_BIRTHDAY(String pH_BIRTHDAY) {
		PH_BIRTHDAY = pH_BIRTHDAY;
	}
	public String getPH_EMAIL() {
		return PH_EMAIL;
	}
	public void setPH_EMAIL(String pH_EMAIL) {
		PH_EMAIL = pH_EMAIL;
	}
	public String getPH_INDUSTRY() {
		return PH_INDUSTRY;
	}
	public void setPH_INDUSTRY(String pH_INDUSTRY) {
		PH_INDUSTRY = pH_INDUSTRY;
	}
	public String getPH_WORKTYPE() {
		return PH_WORKTYPE;
	}
	public void setPH_WORKTYPE(String pH_WORKTYPE) {
		PH_WORKTYPE = pH_WORKTYPE;
	}
	public String getINS_RELATION() {
		return INS_RELATION;
	}
	public void setINS_RELATION(String iNS_RELATION) {
		INS_RELATION = iNS_RELATION;
	}
	public String getINS_NAME() {
		return INS_NAME;
	}
	public void setINS_NAME(String iNS_NAME) {
		INS_NAME = iNS_NAME;
	}
	public String getINS_BIRTHDAY() {
		return INS_BIRTHDAY;
	}
	public void setINS_BIRTHDAY(String iNS_BIRTHDAY) {
		INS_BIRTHDAY = iNS_BIRTHDAY;
	}
	public String getBENE_RELATION() {
		return BENE_RELATION;
	}
	public void setBENE_RELATION(String bENE_RELATION) {
		BENE_RELATION = bENE_RELATION;
	}
	public String getBENE_NAME() {
		return BENE_NAME;
	}
	public void setBENE_NAME(String bENE_NAME) {
		BENE_NAME = bENE_NAME;
	}
	public String getBENE_BIRTHDAY() {
		return BENE_BIRTHDAY;
	}
	public void setBENE_BIRTHDAY(String bENE_BIRTHDAY) {
		BENE_BIRTHDAY = bENE_BIRTHDAY;
	}
	public String getSUCCESS() {
		return SUCCESS;
	}
	public void setSUCCESS(String sUCCESS) {
		SUCCESS = sUCCESS;
	}

	/**
	 * @return Returns the pAY_TYPE.
	 */
	public String getPAY_TYPE() {
		return PAY_TYPE;
	}

	/**
	 * @param pAY_TYPE The pAY_TYPE to set.
	 */
	public void setPAY_TYPE(String pAY_TYPE) {
		PAY_TYPE = pAY_TYPE;
	}

	/**
	 * @return Returns the pAY_PERIOD.
	 */
	public String getPAY_PERIOD() {
		return PAY_PERIOD;
	}

	/**
	 * @param pAY_PERIOD The pAY_PERIOD to set.
	 */
	public void setPAY_PERIOD(String pAY_PERIOD) {
		PAY_PERIOD = pAY_PERIOD;
	}
	
}
