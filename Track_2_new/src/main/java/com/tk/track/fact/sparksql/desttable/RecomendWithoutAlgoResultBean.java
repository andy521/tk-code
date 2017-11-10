package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

/**
 * Created by t-chenhao01 on 2016/12/14.
 */
public class RecomendWithoutAlgoResultBean implements Serializable {

    private static final long serialVersionUID = -4252383293984567112L;
    private String user_id;
    private String member_id;
    private String open_id;
    private String terminal_id;
    private String pcpinfo;
    private String wappinfo;
    private String apppinfo;
    private String wxpinfo;

    public RecomendWithoutAlgoResultBean() {
    }

    @Override
    public String toString() {
        return "RecomendWithoutAlgoResultBean{" +
                "user_id='" + user_id + '\'' +
                ", member_id='" + member_id + '\'' +
                ", open_id='" + open_id + '\'' +
                ", terminal_id='" + terminal_id + '\'' +
                ", pcpinfo='" + pcpinfo + '\'' +
                ", wappinfo='" + wappinfo + '\'' +
                ", apppinfo='" + apppinfo + '\'' +
                ", wxpinfo='" + wxpinfo + '\'' +
                '}';
    }

    public RecomendWithoutAlgoResultBean(String user_id, String member_id, String open_id, String terminal_id, String pcpinfo, String wappinfo, String apppinfo, String wxpinfo) {
        this.user_id = user_id;
        this.member_id = member_id;
        this.open_id = open_id;
        this.terminal_id = terminal_id;
        this.pcpinfo = pcpinfo;
        this.wappinfo = wappinfo;
        this.apppinfo = apppinfo;
        this.wxpinfo = wxpinfo;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getMember_id() {
        return member_id;
    }

    public void setMember_id(String member_id) {
        this.member_id = member_id;
    }

    public String getOpen_id() {
        return open_id;
    }

    public void setOpen_id(String open_id) {
        this.open_id = open_id;
    }

    public String getTerminal_id() {
        return terminal_id;
    }

    public void setTerminal_id(String terminal_id) {
        this.terminal_id = terminal_id;
    }

    public String getPcpinfo() {
        return pcpinfo;
    }

    public void setPcpinfo(String pcpinfo) {
        this.pcpinfo = pcpinfo;
    }

    public String getWappinfo() {
        return wappinfo;
    }

    public void setWappinfo(String wappinfo) {
        this.wappinfo = wappinfo;
    }

    public String getApppinfo() {
        return apppinfo;
    }

    public void setApppinfo(String apppinfo) {
        this.apppinfo = apppinfo;
    }

    public String getWxpinfo() {
        return wxpinfo;
    }

    public void setWxpinfo(String wxpinfo) {
        this.wxpinfo = wxpinfo;
    }
}
