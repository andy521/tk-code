/**
 * @Title: FactTerminalMap.java
 * @Package: com.tk.track.fact.sparksql.desttable
 * @Description: TODO
 * @author zhang.shy
 * @Data 2016年5月7日
 * @version 1.0
 */
package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

/**
 * @ClassName: FactTerminalMap
 * @Description: TODO
 * @author zhang.shy
 * @date   2016年5月7日 
 */
public class FactTerminalMap implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -6175367361996269347L;
    
    private String ROWKEY;        //uuid 自动生成
    private String TERMINAL_ID;   //终端ID,用于标识唯一的终端用户
    private String USER_ID;       //用户标识
    private String CLIENTTIME;          //用户浏览页面日期
    private String APP_TYPE;//APP应用类型
    private String APP_ID;//具体APP应用

    
    /**
     * @Constructor FactTerminalMap
     * @param rOWKEY
     * @param cLIENTTIME
     * @param uSER_ID
     * @param tERMINAL_ID
     */
    public FactTerminalMap(String rOWKEY, String tERMINAL_ID, String uSER_ID, String cLIENTTIME,String appType,String appId) {
        super();
        ROWKEY = rOWKEY;
        TERMINAL_ID = tERMINAL_ID;
        USER_ID = uSER_ID;
        CLIENTTIME = cLIENTTIME;
        this.APP_TYPE = appType;
        this.APP_ID = appId;
    }
    
    /**
     * @Methods: toString
     * @see java.lang.Object#toString() 
     */
    @Override
    public String toString() {
        return "FactTerminalMap [ROWKEY=" + ROWKEY + ", TERMINAL_ID="
                + TERMINAL_ID + ", USER_ID=" + USER_ID + ", CLIENTTIME=" + CLIENTTIME + ",APP_TYPE=" + APP_TYPE + ",APP_ID=" + APP_ID + "]";
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

    public FactTerminalMap() {
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
	
}
