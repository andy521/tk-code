package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

/**
 * 
 * @author itw_wangcy
 *
 */
public class RenewalMessageBean implements Serializable {
    private static final long serialVersionUID = -5428852656174287443L;
    private  String riskname;      
    private  String riskcode;      
    private  String policyno;      
    private  String identifynumber;
    private  String email;         
    private  String mobilephone;   
    private  String fromid;     
    private  String insuredname;   
    private  String platform;      
    private  String cheapoff;      
    private  String enddate ;      
    private  String policytime ;   
    private  String isrenewal;     
    private  String waitdays ;     
    private  String identifytype;  
    private  String  open_id;      
    private  String kxdays ;   
    private  String startdate;
    private String splancode;
    private String splanname;
    private String iswithwaitdays;

    public String getSplanname() {
		return splanname;
	}

	public void setSplanname(String splanname) {
		this.splanname = splanname;
	}

	public String getSplancode() {
		return splancode;
	}

	public void setSplancode(String splancode) {
		this.splancode = splancode;
	}

	public String getStartdate() {
		return startdate;
	}

	public void setStartdate(String startdate) {
		this.startdate = startdate;
	}

	public String getRiskname() {
		return riskname;
	}

	public void setRiskname(String riskname) {
		this.riskname = riskname;
	}

	public String getRiskcode() {
		return riskcode;
	}

	public void setRiskcode(String riskcode) {
		this.riskcode = riskcode;
	}

	public String getPolicyno() {
		return policyno;
	}

	public void setPolicyno(String policyno) {
		this.policyno = policyno;
	}

	public String getIdentifynumber() {
		return identifynumber;
	}

	public void setIdentifynumber(String identifynumber) {
		this.identifynumber = identifynumber;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getMobilephone() {
		return mobilephone;
	}

	public void setMobilephone(String mobilephone) {
		this.mobilephone = mobilephone;
	}

 

	public String getFromid() {
		return fromid;
	}

	public void setFromid(String fromid) {
		this.fromid = fromid;
	}

	public String getInsuredname() {
		return insuredname;
	}

	public void setInsuredname(String insuredname) {
		this.insuredname = insuredname;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}


	public String getCheapoff() {
		return cheapoff;
	}

	public void setCheapoff(String cheapoff) {
		this.cheapoff = cheapoff;
	}

	public String getEnddate() {
		return enddate;
	}

	public void setEnddate(String enddate) {
		this.enddate = enddate;
	}

	public String getPolicytime() {
		return policytime;
	}

	public void setPolicytime(String policytime) {
		this.policytime = policytime;
	}

	public String getIsrenewal() {
		return isrenewal;
	}

	public void setIsrenewal(String isrenewal) {
		this.isrenewal = isrenewal;
	}

	public String getWaitdays() {
		return waitdays;
	}

	public void setWaitdays(String waitdays) {
		this.waitdays = waitdays;
	}

	public String getIdentifytype() {
		return identifytype;
	}

	public void setIdentifytype(String identifytype) {
		this.identifytype = identifytype;
	}

	public String getOpen_id() {
		return open_id;
	}

	public void setOpen_id(String open_id) {
		this.open_id = open_id;
	}

	public String getKxdays() {
		return kxdays;
	}

	public void setKxdays(String kxdays) {
		this.kxdays = kxdays;
	}

	public String getIswithwaitdays() {
		return iswithwaitdays;
	}

	public void setIswithwaitdays(String iswithwaitdays) {
		this.iswithwaitdays = iswithwaitdays;
	}
    
	
	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	public RenewalMessageBean() {
    }

    public RenewalMessageBean(String riskname,String riskcode,String policyno,String identifynumber,
    		String email,String mobilephone,String fromid,String insuredname,String platform,String cheapoff,String enddate,String policytime,String isrenewal,String waitdays,
    		String identifytype,String open_id,String kxdays,String startdate,String splancode,String splanname,String iswithwaitdays) {
    	this.riskname=riskname;
    	this.riskcode=riskcode;
    	this.policyno=policyno;
    	this.identifynumber=identifynumber;
    	this.email=email;
    	this.mobilephone=mobilephone;
    	this.fromid=fromid;
    	this.insuredname=insuredname;
    	this.platform=platform;
    	this.cheapoff=cheapoff;
    	this.enddate=enddate;
    	this.policytime=policytime;
    	this.isrenewal=isrenewal;
    	this.waitdays=waitdays;
    	this.identifytype=identifytype;
    	this.open_id=open_id;
    	this.kxdays=kxdays;
    	this.startdate= startdate;
    	this.splancode= splancode;
    	this.splanname= splanname;
    	this.iswithwaitdays = iswithwaitdays;
    }

	@Override
	public String toString() {
		return "RenewalMessageBean [riskname=" + riskname + ", riskcode=" + riskcode + ", policyno=" + policyno
				+ ", identifynumber=" + identifynumber + ", email=" + email + ", mobilephone=" + mobilephone
				+ ", fromid=" + fromid + ", insuredname=" + insuredname + ", platform=" + platform + ", cheapoff="
				+ cheapoff + ", enddate=" + enddate + ", policytime=" + policytime + ", isrenewal=" + isrenewal
				+ ", waitdays=" + waitdays + ", identifytype=" + identifytype + ", open_id=" + open_id + ", kxdays="
				+ kxdays + "]";
	}



   
}
