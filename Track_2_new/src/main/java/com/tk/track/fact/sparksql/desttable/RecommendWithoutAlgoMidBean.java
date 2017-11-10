package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

/**
 * Created by itw_chenhao01 on 2017/3/9.
 */
public class RecommendWithoutAlgoMidBean implements Serializable {

    private static final long serialVersionUID = 7112470084483405100L;
    private String user_id;
    private String member_id ;
    private  String open_id ;
    private  String terminal_id;
    private  String pinfo ;
    private  String info_type ;

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

    public String getPinfo() {
        return pinfo;
    }

    public void setPinfo(String pinfo) {
        this.pinfo = pinfo;
    }

    public String getInfo_type() {
        return info_type;
    }

    public void setInfo_type(String info_type) {
        this.info_type = info_type;
    }
}
