package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

/**
 * Created by itw_chenhao01 on 2017/2/21.
 */
public class PtradeReferId implements Serializable{

    private String pt_id;

    private String trade_pono;

    private String trade_date;

    private String refer_id;

    private String trade_id;

    public String getTrade_billno() {
        return trade_billno;
    }

    public void setTrade_billno(String trade_billno) {
        this.trade_billno = trade_billno;
    }

    private String trade_billno;

    public String getTrade_id() {
        return trade_id;
    }

    public void setTrade_id(String trade_id) {
        this.trade_id = trade_id;
    }

    public String getPt_id() {
        return pt_id;
    }

    public void setPt_id(String pt_id) {
        this.pt_id = pt_id;
    }

    public String getTrade_pono() {
        return trade_pono;
    }

    public void setTrade_pono(String trade_pono) {
        this.trade_pono = trade_pono;
    }

    public String getTrade_date() {
        return trade_date;
    }

    public void setTrade_date(String trade_date) {
        this.trade_date = trade_date;
    }

    public String getRefer_id() {
        return refer_id;
    }

    public void setRefer_id(String refer_id) {
        this.refer_id = refer_id;
    }
}
