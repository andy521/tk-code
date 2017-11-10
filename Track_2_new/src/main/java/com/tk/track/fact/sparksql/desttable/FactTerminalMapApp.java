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
 * @ClassName: FactTerminalMapApp
 * @Description: TODO
 * @author liran
 * @date   2016年8月19日 
 */
public class FactTerminalMapApp implements Serializable {

    private static final long serialVersionUID = -6175367361996269347L;
    
    private String ROWKEY;        //uuid 自动生成
    private String APP_TYPE;      //APP应用类型
    private String APP_ID;        //具体APP应用
    private String TERMINAL_ID;   //终端ID,用于标识唯一的终端用户
    private String USER_ID;       //用户标识
    private String VISITCOUNT;    //访问次数
    private String LASTTIME;      //最后访问时间
    private String MOBILE;        //手机
    private String NAME;          //姓名
    
	public FactTerminalMapApp() {
		super();
	}

	public FactTerminalMapApp(String rOWKEY, String aPP_TYPE, String aPP_ID,
			String tERMINAL_ID, String uSER_ID, String vISITCOUNT,
			String lASTTIME, String mOBILE, String nAME) {
		super();
		ROWKEY = rOWKEY;
		APP_TYPE = aPP_TYPE;
		APP_ID = aPP_ID;
		TERMINAL_ID = tERMINAL_ID;
		USER_ID = uSER_ID;
		VISITCOUNT = vISITCOUNT;
		LASTTIME = lASTTIME;
		MOBILE = mOBILE;
		NAME = nAME;
	}

	public String getROWKEY() {
		return ROWKEY;
	}
	public void setROWKEY(String rOWKEY) {
		ROWKEY = rOWKEY;
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
	public String getVISITCOUNT() {
		return VISITCOUNT;
	}
	public void setVISITCOUNT(String vISITCOUNT) {
		VISITCOUNT = vISITCOUNT;
	}
	public String getLASTTIME() {
		return LASTTIME;
	}
	public void setLASTTIME(String lASTTIME) {
		LASTTIME = lASTTIME;
	}
	public String getMOBILE() {
		return MOBILE;
	}
	public void setMOBILE(String mOBILE) {
		MOBILE = mOBILE;
	}
	public String getNAME() {
		return NAME;
	}
	public void setNAME(String nAME) {
		NAME = nAME;
	}	
}
