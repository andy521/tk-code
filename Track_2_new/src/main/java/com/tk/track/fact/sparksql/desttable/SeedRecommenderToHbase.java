package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

/**
 * Created by itw_chenhao01 on 2017/8/16.
 */
public class SeedRecommenderToHbase implements Serializable{
    private String unionid;
    private String shareinfo;

    public String getUnionid() {
        return unionid;
    }

    public void setUnionid(String unionid) {
        this.unionid = unionid;
    }

    public String getShareinfo() {
        return shareinfo;
    }

    public void setShareinfo(String shareinfo) {
        this.shareinfo = shareinfo;
    }
}
