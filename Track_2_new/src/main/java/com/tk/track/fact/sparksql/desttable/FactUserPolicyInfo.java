/**
 * @Title: FactUserPolicyInfo.java
 * @Package: com.tk.track.fact.sparksql.desttable
 * @Description: TODO
 * @author zhang.shy
 * @Data 2016年6月5日
 * @version 1.0
 */
package com.tk.track.fact.sparksql.desttable;

/**
 * @ClassName: FactUserPolicyInfo
 * @Description: TODO
 * @author zhang.shy
 * @date   2016年6月5日 
 */
public class FactUserPolicyInfo {

    private String ROWKEY;               //uuid 自动生成
    private String USER_ID;              //用户标识
    private String LRT_ID;               //险种号
    private String LIA_POLICYNO;         //有效保单号
    private String LIA_ACCEPTTIME;       //保单购买时间
    
    
    /**
     * @Constructor FactUserPolicyInfo
     * @param rOWKEY
     * @param uSER_ID
     * @param lRT_ID
     * @param lIA_POLICYNO
     * @param lIA_ACCEPTTIME
     */
    public FactUserPolicyInfo(String rOWKEY, String uSER_ID, String lRT_ID, String lIA_POLICYNO,
            String lIA_ACCEPTTIME) {
        super();
        ROWKEY = rOWKEY;
        USER_ID = uSER_ID;
        LRT_ID = lRT_ID;
        LIA_POLICYNO = lIA_POLICYNO;
        LIA_ACCEPTTIME = lIA_ACCEPTTIME;
    }

    /**
     * @Methods: toString
     * @see java.lang.Object#toString() 
     */
    @Override
    public String toString() {
        return "FactUserPolicyInfo [ROWKEY=" + ROWKEY + ", USER_ID=" + USER_ID + ", LRT_ID=" + LRT_ID
                + ", LIA_POLICYNO=" + LIA_POLICYNO + ", LIA_ACCEPTTIME=" + LIA_ACCEPTTIME + "]";
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
     * @MethodsName: getLRT_ID
     * @return lRT_ID.
     */
    public String getLRT_ID() {
        return LRT_ID;
    }

    /**
     * @MethodsName: setLRT_ID
     * @param lRT_ID 
     */
    public void setLRT_ID(String lRT_ID) {
        LRT_ID = lRT_ID;
    }

    /**
     * @MethodsName: getLIA_POLICYNO
     * @return lIA_POLICYNO.
     */
    public String getLIA_POLICYNO() {
        return LIA_POLICYNO;
    }

    /**
     * @MethodsName: setLIA_POLICYNO
     * @param lIA_POLICYNO 
     */
    public void setLIA_POLICYNO(String lIA_POLICYNO) {
        LIA_POLICYNO = lIA_POLICYNO;
    }

    /**
     * @MethodsName: getLIA_ACCEPTTIME
     * @return lIA_ACCEPTTIME.
     */
    public String getLIA_ACCEPTTIME() {
        return LIA_ACCEPTTIME;
    }

    /**
     * @MethodsName: setLIA_ACCEPTTIME
     * @param lIA_ACCEPTTIME 
     */
    public void setLIA_ACCEPTTIME(String lIA_ACCEPTTIME) {
        LIA_ACCEPTTIME = lIA_ACCEPTTIME;
    }

    public FactUserPolicyInfo() {
    }
}
