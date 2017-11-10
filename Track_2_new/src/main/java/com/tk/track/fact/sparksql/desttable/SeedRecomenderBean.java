package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

/**
 * Created by itw_chenhao01 on 2017/8/15.
 */
public class SeedRecomenderBean implements Serializable{
    private static final long serialVersionUID = -2012741771346591409L;
    private String unionid;
    private String productId;
    private String openId;
    private String visitPage;
    private String visitTime;
    private String productName;

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getUnionid() {
        return unionid;
    }

    public void setUnionid(String unionid) {
        this.unionid = unionid;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getOpenId() {
        return openId;
    }

    public void setOpenId(String openId) {
        this.openId = openId;
    }

    public String getVisitPage() {
        return visitPage;
    }

    public void setVisitPage(String visitPage) {
        this.visitPage = visitPage;
    }

    public String getVisitTime() {
        return visitTime;
    }

    public void setVisitTime(String visitTime) {
        this.visitTime = visitTime;
    }
}
