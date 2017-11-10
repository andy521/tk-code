package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

/**
 * Created by t-chenhao01 on 2016/11/1.
 */
public class RecommendWithoutAlgoToHbaseBean implements Serializable {
    private static final long serialVersionUID = -5428852656174287443L;
    private String user_id;
    private String terminal_id;
    private String open_id;
    private String member_id;
    private String wechat_product_list;
    private String pc_product_list;
    private  String m_product_list;
    private String app_product_list;

    public RecommendWithoutAlgoToHbaseBean() {
    }

    public RecommendWithoutAlgoToHbaseBean(String user_id, String terminal_id, String open_id, String member_id, String wechat_product_list, String pc_product_list, String m_product_list, String app_product_list) {
        this.user_id = user_id;
        this.terminal_id = terminal_id;
        this.open_id = open_id;
        this.member_id = member_id;
        this.wechat_product_list = wechat_product_list;
        this.pc_product_list = pc_product_list;
        this.m_product_list = m_product_list;
        this.app_product_list = app_product_list;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getTerminal_id() {
        return terminal_id;
    }

    public void setTerminal_id(String terminal_id) {
        this.terminal_id = terminal_id;
    }

    public String getOpen_id() {
        return open_id;
    }

    public void setOpen_id(String open_id) {
        this.open_id = open_id;
    }

    public String getMember_id() {
        return member_id;
    }

    public void setMember_id(String member_id) {
        this.member_id = member_id;
    }

    public String getWechat_product_list() {
        return wechat_product_list;
    }

    public void setWechat_product_list(String wechat_product_list) {
        this.wechat_product_list = wechat_product_list;
    }

    public String getPc_product_list() {
        return pc_product_list;
    }

    public void setPc_product_list(String pc_product_list) {
        this.pc_product_list = pc_product_list;
    }

    public String getM_product_list() {
        return m_product_list;
    }

    public void setM_product_list(String m_product_list) {
        this.m_product_list = m_product_list;
    }

    public String getApp_product_list() {
        return app_product_list;
    }

    public void setApp_product_list(String app_product_list) {
        this.app_product_list = app_product_list;
    }
}
