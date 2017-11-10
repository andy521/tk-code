package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

/**
 * Created by t-chenhao01 on 2016/9/13.
 */
public class SeedIdProduct implements Serializable{
    private String PRODUCT_ID;
    private String PRODUCT_NAME;
    private String SEED_ID;
    private String SEED_TYPE;
    private String SEED_URL;
    private String CLICK_TIME;
    private String refer_id;

    public String getRefer_id() {
        return refer_id;
    }

    public void setRefer_id(String refer_id) {
        this.refer_id = refer_id;
    }

    public SeedIdProduct() {
    }

    public SeedIdProduct(String PRODUCT_ID, String PRODUCT_NAME, String SEED_ID, String SEED_TYPE, String SEED_URL,
                         String CLICK_TIME,String refer_id) {
        this.PRODUCT_ID = PRODUCT_ID;
        this.PRODUCT_NAME = PRODUCT_NAME;
        this.SEED_ID = SEED_ID;
        this.SEED_TYPE = SEED_TYPE;
        this.SEED_URL = SEED_URL;
        this.CLICK_TIME = CLICK_TIME;
        this.refer_id=refer_id;
    }

    public String getPRODUCT_ID() {
        return PRODUCT_ID;
    }

    public void setPRODUCT_ID(String PRODUCT_ID) {
        this.PRODUCT_ID = PRODUCT_ID;
    }

    public String getPRODUCT_NAME() {
        return PRODUCT_NAME;
    }

    public void setPRODUCT_NAME(String PRODUCT_NAME) {
        this.PRODUCT_NAME = PRODUCT_NAME;
    }

    public String getSEED_ID() {
        return SEED_ID;
    }

    public void setSEED_ID(String SEED_ID) {
        this.SEED_ID = SEED_ID;
    }

    public String getSEED_TYPE() {
        return SEED_TYPE;
    }

    public void setSEED_TYPE(String SEED_TYPE) {
        this.SEED_TYPE = SEED_TYPE;
    }

    public String getSEED_URL() {
        return SEED_URL;
    }

    public void setSEED_URL(String SEED_URL) {
        this.SEED_URL = SEED_URL;
    }

    public String getCLICK_TIME() {
        return CLICK_TIME;
    }

    public void setCLICK_TIME(String CLICK_TIME) {
        this.CLICK_TIME = CLICK_TIME;
    }
}
