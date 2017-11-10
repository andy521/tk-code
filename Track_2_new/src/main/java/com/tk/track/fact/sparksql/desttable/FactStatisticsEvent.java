/**
 * @Title: FactStatisticsEvent.java
 * @Package: com.tk.track.fact.sparksql.desttable
 * @Description: TODO
 * @author zhang.shy
 * @Data 2016年5月7日
 * @version 1.0
 */
package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

/**
 * @ClassName: FactStatisticsEvent
 * @Description: TODO
 * @author zhang.shy
 * @date   2016年5月7日 
 */
public class FactStatisticsEvent implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 2935702935113801019L;

    private String ROWKEY;         //uuid 自动生成
    private String USER_ID;        //用户标识
    private String APP_TYPE;       //标记日志log的来源渠道
    private String APP_ID;         //对appType的细分
    private String EVENT;          //用户行为
    private String SUBTYPE;        //用户具体行为
    private String LABEL;          //自定义的用户行为信息
    private String CUSTOM_VAL;     //自定义内容
    private String VISIT_COUNT;    //浏览次数
    private String VISIT_TIME;     //当日第一次浏览时间
    private String VISIT_DURATION; //浏览页面的总时长
    private String FROM_ID;
 
    
    /**
     * @Constructor FactStatisticsEvent
     * @param rOWKEY
     * @param uSER_ID
     * @param aPP_TYPE
     * @param aPP_ID
     * @param eVENT
     * @param sUBTYPE
     * @param lABEL
     * @param cUSTOM_VAL
     * @param vISIT_COUNT
     * @param vISIT_TIME
     * @param vISIT_DURATION
     */
    public FactStatisticsEvent(String rOWKEY, String uSER_ID, String aPP_TYPE, String aPP_ID, String eVENT,
            String sUBTYPE, String lABEL, String cUSTOM_VAL, String vISIT_COUNT, String vISIT_TIME,
            String vISIT_DURATION,String fromId) {
        super();
        ROWKEY = rOWKEY;
        USER_ID = uSER_ID;
        APP_TYPE = aPP_TYPE;
        APP_ID = aPP_ID;
        EVENT = eVENT;
        SUBTYPE = sUBTYPE;
        LABEL = lABEL;
        CUSTOM_VAL = cUSTOM_VAL;
        VISIT_COUNT = vISIT_COUNT;
        VISIT_TIME = vISIT_TIME;
        VISIT_DURATION = vISIT_DURATION;
        this.FROM_ID = fromId;
    }

    /**
     * @Methods: toString
     * @see java.lang.Object#toString() 
     */
    @Override
    public String toString() {
        return "FactStatisticsEvent [ROWKEY=" + ROWKEY + ", USER_ID=" + USER_ID + ", APP_TYPE=" + APP_TYPE + ", APP_ID="
                + APP_ID + ", EVENT=" + EVENT + ", SUBTYPE=" + SUBTYPE + ", LABEL=" + LABEL + ", CUSTOM_VAL="
                + CUSTOM_VAL + ", VISIT_COUNT=" + VISIT_COUNT + ", VISIT_TIME=" + VISIT_TIME + ", VISIT_DURATION="
                + VISIT_DURATION + ",FROM_ID=" + FROM_ID + "]";
    }

    /**
     * @MethodsName: getROWKEY
     * @return rOWKEY.
     */
    public String getROWKEY() {
        return ROWKEY;
    }

    /**
     * @MethodsName: setROWKEY
     * @param rOWKEY 
     */
    public void setROWKEY(String rOWKEY) {
        ROWKEY = rOWKEY;
    }

    /**
     * @MethodsName: getUSER_ID
     * @return uSER_ID.
     */
    public String getUSER_ID() {
        return USER_ID;
    }

    /**
     * @MethodsName: setUSER_ID
     * @param uSER_ID 
     */
    public void setUSER_ID(String uSER_ID) {
        USER_ID = uSER_ID;
    }

    /**
     * @MethodsName: getAPP_TYPE
     * @return aPP_TYPE.
     */
    public String getAPP_TYPE() {
        return APP_TYPE;
    }

    /**
     * @MethodsName: setAPP_TYPE
     * @param aPP_TYPE 
     */
    public void setAPP_TYPE(String aPP_TYPE) {
        APP_TYPE = aPP_TYPE;
    }

    /**
     * @MethodsName: getAPP_ID
     * @return aPP_ID.
     */
    public String getAPP_ID() {
        return APP_ID;
    }

    /**
     * @MethodsName: setAPP_ID
     * @param aPP_ID 
     */
    public void setAPP_ID(String aPP_ID) {
        APP_ID = aPP_ID;
    }

    /**
     * @MethodsName: getEVENT
     * @return eVENT.
     */
    public String getEVENT() {
        return EVENT;
    }

    /**
     * @MethodsName: setEVENT
     * @param eVENT 
     */
    public void setEVENT(String eVENT) {
        EVENT = eVENT;
    }

    /**
     * @MethodsName: getSUBTYPE
     * @return sUBTYPE.
     */
    public String getSUBTYPE() {
        return SUBTYPE;
    }

    /**
     * @MethodsName: setSUBTYPE
     * @param sUBTYPE 
     */
    public void setSUBTYPE(String sUBTYPE) {
        SUBTYPE = sUBTYPE;
    }

    /**
     * @MethodsName: getLABEL
     * @return lABEL.
     */
    public String getLABEL() {
        return LABEL;
    }

    /**
     * @MethodsName: setLABEL
     * @param lABEL 
     */
    public void setLABEL(String lABEL) {
        LABEL = lABEL;
    }

    /**
     * @MethodsName: getCUSTOM_VAL
     * @return cUSTOM_VAL.
     */
    public String getCUSTOM_VAL() {
        return CUSTOM_VAL;
    }

    /**
     * @MethodsName: setCUSTOM_VAL
     * @param cUSTOM_VAL 
     */
    public void setCUSTOM_VAL(String cUSTOM_VAL) {
        CUSTOM_VAL = cUSTOM_VAL;
    }

    /**
     * @MethodsName: getVISIT_COUNT
     * @return vISIT_COUNT.
     */
    public String getVISIT_COUNT() {
        return VISIT_COUNT;
    }

    /**
     * @MethodsName: setVISIT_COUNT
     * @param vISIT_COUNT 
     */
    public void setVISIT_COUNT(String vISIT_COUNT) {
        VISIT_COUNT = vISIT_COUNT;
    }

    /**
     * @MethodsName: getVISIT_TIME
     * @return vISIT_TIME.
     */
    public String getVISIT_TIME() {
        return VISIT_TIME;
    }

    /**
     * @MethodsName: setVISIT_TIME
     * @param vISIT_TIME 
     */
    public void setVISIT_TIME(String vISIT_TIME) {
        VISIT_TIME = vISIT_TIME;
    }

    /**
     * @MethodsName: getVISIT_DURATION
     * @return vISIT_DURATION.
     */
    public String getVISIT_DURATION() {
        return VISIT_DURATION;
    }

    
    
    
    public String getFROM_ID() {
		return FROM_ID;
	}

	public void setFROM_ID(String fROM_ID) {
		FROM_ID = fROM_ID;
	}

	/**
     * @MethodsName: setVISIT_DURATION
     * @param vISIT_DURATION 
     */
    public void setVISIT_DURATION(String vISIT_DURATION) {
        VISIT_DURATION = vISIT_DURATION;
    }

    public FactStatisticsEvent() {
    }
}
