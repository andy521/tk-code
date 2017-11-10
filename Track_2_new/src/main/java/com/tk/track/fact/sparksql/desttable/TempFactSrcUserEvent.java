package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

public class TempFactSrcUserEvent implements Serializable, Cloneable {

    private static final long serialVersionUID = 6878586178315708875L;

    private String IP;
    private String TIME;
    private String APP_TYPE;
    private String APP_ID;
    private String USER_ID;
    private String PAGE;
    private String EVENT;
    private String SUBTYPE;
    private String LABEL;
    private String CUSTOM_VAL;
    private String REFER;
    private String CURRENTURL;
    private String BROWSER;
    private String SYSINFO;
    private String TERMINAL_ID;
    private String DURATION;
    private String CLIENTTIME;
    private String FROM_ID;
    
    public TempFactSrcUserEvent(String iP, String tIME, String aPP_TYPE,
			String aPP_ID, String uSER_ID, String pAGE, String eVENT,
			String sUBTYPE, String lABEL, String cUSTOM_VAL, String rEFER,
			String cURRENTURL, String bROWSER, String sYSINFO,
			String tERMINAL_ID, String dURATION, String cLIENTTIME,
			String fROM_ID) {
		super();
		IP = iP;
		TIME = tIME;
		APP_TYPE = aPP_TYPE;
		APP_ID = aPP_ID;
		USER_ID = uSER_ID;
		PAGE = pAGE;
		EVENT = eVENT;
		SUBTYPE = sUBTYPE;
		LABEL = lABEL;
		CUSTOM_VAL = cUSTOM_VAL;
		REFER = rEFER;
		CURRENTURL = cURRENTURL;
		BROWSER = bROWSER;
		SYSINFO = sYSINFO;
		TERMINAL_ID = tERMINAL_ID;
		DURATION = dURATION;
		CLIENTTIME = cLIENTTIME;
		FROM_ID = fROM_ID;
	}

	/**
     * @Methods: toString
     * @see java.lang.Object#toString() 
     */
    @Override
    public String toString() {
        return "FactSrcUserEvent [IP=" + IP + ", TIME=" + TIME + ", APP_TYPE=" + APP_TYPE
                + ", APP_ID=" + APP_ID + ", USER_ID=" + USER_ID + ", PAGE=" + PAGE + ", EVENT=" + EVENT + ", SUBTYPE="
                + SUBTYPE + ", LABEL=" + LABEL + ", CUSTOM_VAL=" + CUSTOM_VAL + ", REFER=" + REFER + ", CURRENTURL="
                + CURRENTURL + ", BROWSER=" + BROWSER + ", SYSINFO=" + SYSINFO + ", TERMINAL_ID=" + TERMINAL_ID
                + ", DURATION=" + DURATION + ", CLIENTTIME=" + CLIENTTIME +",FROM_ID=" + FROM_ID + "]";
    }

    /**
     * @MethodsName: getIP
     * @return iP.
     */
    public String getIP() {
        return IP;
    }

    /**
     * @MethodsName: setIP
     * @param iP 
     */
    public void setIP(String iP) {
        IP = iP;
    }

    /**
     * @MethodsName: getTIME
     * @return tIME.
     */
    public String getTIME() {
        return TIME;
    }

    /**
     * @MethodsName: setTIME
     * @param tIME 
     */
    public void setTIME(String tIME) {
        TIME = tIME;
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
     * @MethodsName: getPAGE
     * @return pAGE.
     */
    public String getPAGE() {
        return PAGE;
    }

    /**
     * @MethodsName: setPAGE
     * @param pAGE 
     */
    public void setPAGE(String pAGE) {
        PAGE = pAGE;
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
     * @MethodsName: getREFER
     * @return rEFER.
     */
    public String getREFER() {
        return REFER;
    }

    /**
     * @MethodsName: setREFER
     * @param rEFER 
     */
    public void setREFER(String rEFER) {
        REFER = rEFER;
    }

    /**
     * @MethodsName: getCURRENTURL
     * @return cURRENTURL.
     */
    public String getCURRENTURL() {
        return CURRENTURL;
    }

    /**
     * @MethodsName: setCURRENTURL
     * @param cURRENTURL 
     */
    public void setCURRENTURL(String cURRENTURL) {
        CURRENTURL = cURRENTURL;
    }

    /**
     * @MethodsName: getBROWSER
     * @return bROWSER.
     */
    public String getBROWSER() {
        return BROWSER;
    }

    /**
     * @MethodsName: setBROWSER
     * @param bROWSER 
     */
    public void setBROWSER(String bROWSER) {
        BROWSER = bROWSER;
    }

    /**
     * @MethodsName: getSYSINFO
     * @return sYSINFO.
     */
    public String getSYSINFO() {
        return SYSINFO;
    }

    /**
     * @MethodsName: setSYSINFO
     * @param sYSINFO 
     */
    public void setSYSINFO(String sYSINFO) {
        SYSINFO = sYSINFO;
    }

    /**
     * @MethodsName: getTERMINAL_ID
     * @return tERMINAL_ID.
     */
    public String getTERMINAL_ID() {
        return TERMINAL_ID;
    }

    /**
     * @MethodsName: setTERMINAL_ID
     * @param tERMINAL_ID 
     */
    public void setTERMINAL_ID(String tERMINAL_ID) {
        TERMINAL_ID = tERMINAL_ID;
    }

    /**
     * @MethodsName: getDURATION
     * @return dURATION.
     */
    public String getDURATION() {
        return DURATION;
    }

    /**
     * @MethodsName: setDURATION
     * @param dURATION 
     */
    public void setDURATION(String dURATION) {
        DURATION = dURATION;
    }

    /**
     * @MethodsName: getCLIENTTIME
     * @return cLIENTTIME.
     */
    public String getCLIENTTIME() {
        return CLIENTTIME;
    }

    /**
     * @MethodsName: setCLIENTTIME
     * @param cLIENTTIME 
     */
    public void setCLIENTTIME(String cLIENTTIME) {
        CLIENTTIME = cLIENTTIME;
    }

    
    public String getFROM_ID() {
		return FROM_ID;
	}

	public void setFROM_ID(String fROM_ID) {
		FROM_ID = fROM_ID;
	}

	public TempFactSrcUserEvent() {
    }
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

}
